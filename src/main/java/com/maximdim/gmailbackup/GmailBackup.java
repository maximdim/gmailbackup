package com.maximdim.gmailbackup;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.mail.Address;
import javax.mail.FetchProfile;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.Message.RecipientType;
import javax.mail.MessageRemovedException;
import javax.mail.MessagingException;
import javax.mail.search.AndTerm;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.FlagTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.SearchTerm;

import com.google.code.samples.oauth2.OAuth2Authenticator;
import com.sun.mail.imap.IMAPFolder;
import com.sun.mail.imap.IMAPStore;
import com.sun.mail.util.FolderClosedIOException;
import com.sun.mail.util.MessageRemovedIOException;

public class GmailBackup {
  private static final String USER_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
  // timestamps written before we started tracking the time of day
  private static final String USER_TIMESTAMP_FORMAT_LEGACY = "yyyy-MM-dd";
  private final String serviceAccountId;
  private final File serviceAccountPkFile;
  private final String domain;
  private final File timestampFile;
  // <User, Date>
  private final Map<String, Date> userTimestamps;

  private final List<String> users;
  private final List<String> ignoreFrom;
  private final int maxPerRun;
  private final boolean zip;
  private final boolean gzip;
  private final int fetchWindowDays;
  private final int threads;
  // files actually written this run, across every user - incremented from all the backup threads
  private final AtomicInteger filesCreated = new AtomicInteger();
  
  // storage format:
  // dataDir/domain/year/month/day/user_timestamp.mail
  private final File dataDir;

  public GmailBackup(Properties p) {
    this.serviceAccountId = p.getProperty("serviceAccountId");
    this.serviceAccountPkFile = new File(p.getProperty("serviceAccountPkFile"));
    this.domain = p.getProperty("domain");
    this.timestampFile = new File(p.getProperty("timestampFile"));
    this.users = Arrays.asList(p.getProperty("users").split(","));
    this.ignoreFrom = Arrays.asList(p.getProperty("ignoreFrom").split(","));
    this.maxPerRun = Integer.parseInt(p.getProperty("maxPerRun", "1000"));
    this.zip = Boolean.parseBoolean(p.getProperty("zip"));
    this.gzip = Boolean.parseBoolean(p.getProperty("gzip"));
    if (this.zip && this.gzip) {
      throw new IllegalStateException("Both zip and gzip compression specified. Choose one");
    }
    this.fetchWindowDays = Integer.parseInt(p.getProperty("fetchWindowDays", "30"));
    this.threads = Integer.parseInt(p.getProperty("threads", "4"));
    if (this.threads < 1) {
      throw new IllegalStateException("threads must be at least 1");
    }

    Date oldestDate = getDate(p.getProperty("oldestDate", "2012/01/01"), "yyyy-MM-dd");
    this.userTimestamps = loadTimestamp(this.timestampFile, oldestDate);
    
    this.dataDir = new File(p.getProperty("dataDir"));
    // log all properties
    System.out.println("Configuration:");
    System.out.println("serviceAccountId: " + this.serviceAccountId);
    System.out.println("serviceAccountPkFile: " + this.serviceAccountPkFile.getAbsolutePath());
    System.out.println("domain: " + this.domain);
    System.out.println("timestampFile: " + this.timestampFile.getAbsolutePath());
    System.out.println("users: " + this.users);
    System.out.println("ignoreFrom: " + this.ignoreFrom);
    System.out.println("maxPerRun: " + this.maxPerRun);
    System.out.println("zip: " + this.zip);
    System.out.println("gzip: " + this.gzip);
    System.out.println("threads: " + this.threads);
  }

  private int backup() throws Exception {
    OAuth2Authenticator.initialize();

    // users are independent of each other - the only shared state is userTimestamps and the file
    // it is written to. The work is almost entirely waiting on Gmail, so this is close to a
    // straight wall clock division. Set threads=1 to go back to running them one at a time.
    ExecutorService pool = Executors.newFixedThreadPool(this.threads);
    List<Future<?>> futures = new ArrayList<>();
    for(final String user: this.users) {
      futures.add(pool.submit(new Runnable() {
        @Override
        public void run() {
          backupUser(user);
        }
      }));
    }
    pool.shutdown();
    for(Future<?> f: futures) {
      try {
        f.get(); // backupUser reports its own failures - this only surfaces the unexpected ones
      }
      catch (ExecutionException e) {
        System.err.println("Unexpected failure in backup task: "+e.getCause());
        e.getCause().printStackTrace(System.err);
      }
    }
    System.out.println("Done\n");
    return this.filesCreated.get();
  }

  private void backupUser(String user) {
    IMAPStore store = null;
    try {
      log(user, "### Backing up");
      String email = user + "@" + this.domain;
      store = getStore(user, email);
      if (store == null) {
        log(user, "Store is null, skip");
        return;
      }

      Date fetchFrom;
      synchronized (this.userTimestamps) {
        fetchFrom = this.userTimestamps.get(user);
      }
      UserMessagesIterator iterator = new UserMessagesIterator(user, store, fetchFrom);
      int count = 0;
      while(iterator.hasNext() && count < this.maxPerRun) {
        try {
          Message message = iterator.next();
          File f = generateFileName(user, message);
          boolean fileExists = f.exists();
          if (!fileExists) {
            saveMessage(message, f);
            this.filesCreated.incrementAndGet(); // only once the write actually succeeded
          }
          // update stats. Messages are processed in receivedDate order, so this is the exact
          // point the next run has to resume from - no rounding, or a user with more than
          // maxPerRun messages in a single day could never advance past that day.
          synchronized (this.userTimestamps) {
            this.userTimestamps.put(user, message.getReceivedDate());
          }
          log(user, iterator.getStats() + " " + f.getAbsolutePath() + (fileExists ? ": EXISTS" : ""));
          count++;
        }
        catch (MessageRemovedIOException e) {
          System.err.println("["+user+"] Message removed, skipping: "+e);
        }
        catch(FolderClosedIOException e) {
          // connection dropped by the server - next run resumes from the saved timestamp
          System.err.println("["+user+"] Folder closed after "+count+" messages, stopping: "+e);
          break;
        }
      }
      if (count > 0) {
        saveTimestamp(this.userTimestamps, this.timestampFile);
      }
    }
    catch (Exception e) {
      System.err.println("Error getting mail for user ["+user+"]: "+e.getClass().getSimpleName()+": "+e.getMessage());
      e.printStackTrace(System.err);
    }
    finally {
      if (store != null) {
        // closing is best effort - it must not take down the other users
        try {
          store.close();
        }
        catch (MessagingException e) {
          log(user, "Error closing store: "+e);
        }
      }
    }
  }

  private static void log(String user, String message) {
    System.out.println("["+user+"] "+message);
  }

  private File saveMessage(Message message, File f) throws Exception {
    // createDirectories rather than mkdirs - two users can be creating the same day directory at
    // the same time, and mkdirs reports failure when it loses that race
    Files.createDirectories(f.getParentFile().toPath());
    if (this.zip) {
      writeZip(f, message);
    }
    else if (this.gzip) {
      writeGzip(f, message);
    }
    else {
      writeFile(f, message);
    }
    return f;
  }

  private void writeGzip(File f, Message message) throws IOException, MessagingException {
    try (GZIPOutputStream zos = new GZIPOutputStream(new FileOutputStream(f))) {
      message.writeTo(zos);
    }
  }
  
  private void writeZip(File f, Message message) throws IOException, MessagingException {
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(f))) {
      ZipEntry zipEntry = new ZipEntry(f.getName());
      zos.putNextEntry(zipEntry);
      message.writeTo(zos);
      zos.closeEntry();
    }
  }
  
  private void writeFile(File f, Message message) throws IOException, MessagingException {
    try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
      message.writeTo(os);
      os.flush();
    }
  }

  // Format: user_yyyymmddThhmmss_hash.mail
  private File generateFileName(String user, Message message) throws MessagingException {
    // generate folder
    Calendar c = Calendar.getInstance();
    c.setTime(message.getReceivedDate());
    String year = Integer.toString(c.get(Calendar.YEAR));
    String month = Integer.toString(c.get(Calendar.MONTH)+1);
    String day = Integer.toString(c.get(Calendar.DAY_OF_MONTH));
    if (month.length() < 2) month = "0"+month;
    if (day.length() < 2) day = "0"+day;
    
    File folder = new File(this.dataDir, this.domain);
    folder = new File(folder, year);
    folder = new File(folder, month);
    folder = new File(folder, day);

    // generate name
    StringBuilder sb = new StringBuilder();
    sb.append(user);
    sb.append("_");
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
    sb.append(df.format(message.getReceivedDate()));
    sb.append("_");
    sb.append(getHash(message));
    sb.append(".mail");
    if (this.zip) {
      sb.append(".zip");
    }
    else if (this.gzip) {
      sb.append(".gz");
    }

    return new File(folder, sb.toString());
  }
  
  private String getHash(Message m) throws MessagingException {
    String from = m.getFrom() != null && m.getFrom().length > 0? m.getFrom()[0].toString() : "";
    String subject = m.getSubject() != null ? m.getSubject() : "";
    String hash = md5Hex(from + subject);
    // no need to be super long - the hash part is there just to avoid (infrequent) name collisions
    return hash.substring(0, 5);
  }

  /**
   * Lowercase hex MD5 of the UTF-8 bytes - byte for byte what commons-codec DigestUtils.md5Hex()
   * produced. This ends up in file names, so it can never change: every message already on disk
   * would be saved again under a new name.
   */
  static String md5Hex(String s) {
    byte[] digest;
    try {
      digest = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
    }
    catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 not available", e); // required of every JRE
    }
    StringBuilder sb = new StringBuilder(digest.length * 2);
    for (byte b : digest) {
      sb.append(Character.forDigit((b >> 4) & 0xf, 16));
      sb.append(Character.forDigit(b & 0xf, 16));
    }
    return sb.toString();
  }
  
  private IMAPStore getStore(String user, String email) throws Exception {
    String authToken = OAuth2Authenticator.getToken(this.serviceAccountPkFile, this.serviceAccountId, email);
    if (authToken == null) {
      log(user, "authToken null!");
      return null;
    }
    log(user, "authToken OK");

    IMAPStore store = OAuth2Authenticator.connectToImap("imap.gmail.com", 993, email, authToken, false);
    log(user, "imapStore OK");
    return store;
  }
  
  private final Date getDate(String d, String format) {
    SimpleDateFormat df = new SimpleDateFormat(format);
    try {
      return df.parse(d);
    } 
    catch (ParseException e) {
      throw new RuntimeException("Unable to parse Date ["+d+"]", e);
    }
  }
  
  /**
   * load saved timestamp file (if available)
   */
  Map<String, Date> loadTimestamp(File f, Date defaultDate) {
    Map<String, Date> result = new LinkedHashMap<>();
    if (f.exists() && f.canRead()) {
      try (BufferedReader br = new BufferedReader(new FileReader(f))) {
        String line = null;
        SimpleDateFormat df = new SimpleDateFormat(USER_TIMESTAMP_FORMAT);
        SimpleDateFormat dfLegacy = new SimpleDateFormat(USER_TIMESTAMP_FORMAT_LEGACY);
        while((line = br.readLine()) != null) {
          if (line.trim().isEmpty()) {
            continue;
          }
          String[] ss = line.split("=");
          if (ss.length != 2) {
            System.err.println("Don't understand line ["+line+"]");
            continue;
          }
          try {
            String user = ss[0];
            if (this.users.contains(user)) { // filter out users that are no longer being fetched
              result.put(user, parseTimestamp(df, dfLegacy, ss[1]));
            } else {
              System.out.println("Ignore timestamp for user " + user);
            }
          } 
          catch (ParseException e) {
            System.err.println("Unable to parse date ["+ss[1]+"]");
          }
        }
      } 
      catch (IOException e) {
        System.err.println("Error loading user timestamps from "+f.getAbsolutePath()+": "+e.getMessage());
      }
    }
    // fill with defaults
    for(String user: this.users) {
      if (!result.containsKey(user)) {
        result.put(user, defaultDate);
      }
    }
    // log
    System.out.println("Loading timestamps: " + result.size());
    for(Map.Entry<String, Date> me: result.entrySet()) {
      System.out.println(me.getKey()+"="+me.getValue());
    }
    return result;
  }
  
  static Date parseTimestamp(SimpleDateFormat df, SimpleDateFormat dfLegacy, String s) throws ParseException {
    try {
      return df.parse(s);
    }
    catch (ParseException e) {
      return dfLegacy.parse(s);
    }
  }

  class UserMessagesIterator implements Iterator<Message> {
    private final List<Message> messages;
    private final String user;
    private int index;

    public UserMessagesIterator(String user, IMAPStore store, Date fetchFrom) throws MessagingException {
      this.user = user;
      this.messages = getMessages(store, fetchFrom);
    }

    public String getStats() {
      return this.index+"/"+this.messages.size();
    }
    
    @Override
    public boolean hasNext() {
      return this.index < this.messages.size();
    }

    @Override
    public Message next() {
      return this.messages.get(this.index++);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private List<Message> getMessages(IMAPStore store, Date fetchFrom) throws MessagingException {
      IMAPFolder folder = (IMAPFolder)store.getFolder("[Gmail]/All Mail");
      folder.open(Folder.READ_ONLY);
      log(this.user, "imap folder open OK: " + folder.getName());
      int totalMessages = folder.getMessageCount();
      log(this.user, "Total messages: " + totalMessages);

      List<Message> result = new ArrayList<Message>();
      for(Message m: fetch(folder, fetchFrom)) {
        try {
          if (m.getReceivedDate() == null) {
            log(this.user, "Message received date is null: "+m.getSubject());
            continue;
          }
          if (m.getReceivedDate().before(fetchFrom)) {
            //log(this.user, "Message date "+m.getReceivedDate()+" is before "+fetchFrom);
            continue;
          }
          if (shouldInclude(m.getFrom(), getRecipients(m))) {
            result.add(m);
          }
        }
        catch (MessageRemovedException e) {
          log(this.user, "Message already removed: "+e.getMessage());
        }
      }
      log(this.user, "Result filtered to: " + result.size());
      return result;
    }

    /**
     * TO/CC/BCC are served from the prefetched ENVELOPE. Message.getAllRecipients() also asks for
     * NEWSGROUPS, which IMAPMessage doesn't override, so it falls back to reading the Newsgroups
     * header - one FETCH round trip per message.
     */
    private Address[] getRecipients(Message m) throws MessagingException {
      List<Address> result = new ArrayList<>();
      for (RecipientType type : new RecipientType[] { RecipientType.TO, RecipientType.CC, RecipientType.BCC }) {
        Address[] addresses = m.getRecipients(type);
        if (addresses != null) {
          result.addAll(Arrays.asList(addresses));
        }
      }
      return result.toArray(new Address[result.size()]);
    }

    boolean shouldInclude(Address[] from, Address[] to) {
      List<Address> candidates = new ArrayList<>();
      if (from != null && from.length > 0) {
        candidates.addAll(Arrays.asList(from));
      }
      if (to != null && to.length > 0) {
        candidates.addAll(Arrays.asList(to));
      }
      if (candidates.isEmpty()) {
        return false;
      }

      for (String ignore: ignoreFrom) {
        for (Address a : candidates) {
          String addressString = a.toString().toLowerCase();
          if (addressString.contains(ignore)) {
            log(this.user, "Ignoring email with address " + a);
            return false;
          }
        }
      }
      return true;
    }

    private Date getDateDaysFrom(Date from) {
      Calendar c = Calendar.getInstance();
      c.setTime(from);
      c.add(Calendar.DAY_OF_YEAR, fetchWindowDays);
      return c.getTime();
    }
    
    private Message[] fetch(IMAPFolder folder, Date fetchFrom) throws MessagingException {
      // IMAP SEARCH disregards time and timezone, so the server side bound has to be a whole day
      // earlier than the timestamp we resume from. getMessages() then applies the exact cutoff.
      Date searchFrom = searchWindowStart(fetchFrom);
      // Gmail seems to be returning strange result with ComparisonTerm.GE
      SearchTerm st = new ReceivedDateTerm(ComparisonTerm.GT, searchFrom);
      log(this.user, "Setting fetchFrom to "+fetchFrom+" (searching from "+searchFrom+")");

      // drafts live in All Mail too - let the server filter them out (UNDRAFT)
      st = new AndTerm(st, new FlagTerm(new Flags(Flags.Flag.DRAFT), false));

      Date fetchTo = getDateDaysFrom(fetchFrom);
      if (fetchTo.before(new Date())) {
        SearchTerm stTo = new ReceivedDateTerm(ComparisonTerm.LT, fetchTo);
        st = new AndTerm(st, stTo);
        log(this.user, "Setting fetchTo to "+fetchTo);
      }
      
      // IMAP search command disregards time, only date is used
      Message[] messages = folder.search(st);
      //Message[] messages = folder.getMessages();
      log(this.user, "Search returned: " + messages.length);
      
      if (messages.length == 0 && fetchTo.before(new Date())) { // our search window could be too much in the past, retry
        log(this.user, "Retrying with fetchFrom: "+fetchTo);
        return fetch(folder, fetchTo);
      }
      
      FetchProfile fp = new FetchProfile();
      fp.add(FetchProfile.Item.ENVELOPE);
      folder.fetch(messages, fp);

      // messages returned from search not in order. Since we might not process all of them at once
      // we need to sort - by receivedDate, the same field the resume timestamp is taken from.
      Arrays.sort(messages, new Comparator<Message>() {
        @Override
        public int compare(Message m1, Message m2) {
          try {
            long d1 = m1.getReceivedDate() != null ? m1.getReceivedDate().getTime() : 0;
            long d2 = m2.getReceivedDate() != null ? m2.getReceivedDate().getTime() : 0;
            return Long.compare(d1, d2);
          }
          catch (MessagingException e) {
            throw new RuntimeException("Comparator error: "+e.getMessage(), e);
          }
        }
      });

      return messages;
    }
    
  }

  /**
   * Written to a temp file and renamed into place. Opening the real file for writing would truncate
   * it, and a crash at that point leaves it empty or half written - which reads back as "no
   * timestamps", sending every user back to oldestDate and re-walking years of All Mail.
   */
  synchronized void saveTimestamp(Map<String, Date> data, File f) {
    // one writer at a time: every user shares the same temp file and the same target
    Map<String, Date> snapshot;
    synchronized (data) {
      snapshot = new LinkedHashMap<>(data);
    }
    SimpleDateFormat df = new SimpleDateFormat(USER_TIMESTAMP_FORMAT);
    StringBuilder sb = new StringBuilder();
    for(Map.Entry<String, Date> me: snapshot.entrySet()) {
      sb.append(me.getKey()).append("=").append(df.format(me.getValue())).append("\n");
    }
    String content = sb.toString();
    // printed in a single call so the block does not interleave with other users' output
    System.out.print("Saving timestamps: " + snapshot.size() + "\n" + content);

    File tmp = new File(f.getAbsoluteFile().getParentFile(), f.getName()+".tmp");
    try {
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmp))) {
        bw.write(content);
        bw.flush();
      }
      // same directory, so the rename is atomic - readers see either the old file or the new one
      Files.move(tmp.toPath(), f.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
    catch (IOException e) {
      System.err.println("Error saving user timestamps to "+f.getAbsolutePath()+": "+e.getMessage());
      tmp.delete();
    }
  }

  /**
   * Start of the day before the given timestamp - the widest bound an IMAP SEARCH can express
   * without risking dropping messages to date granularity or timezone skew.
   */
  static Date searchWindowStart(Date d) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(Math.min(d.getTime(), new Date().getTime()));
    cal.add(Calendar.DAY_OF_YEAR, -1); // previous day
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return new Date(cal.getTimeInMillis());
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: "+GmailBackup.class.getSimpleName()+" <properties file>");
      System.exit(1);
    }
    File propFile = new File(args[0]);
    if (!propFile.exists() || !propFile.canRead()) {
      System.err.println("Can't read from properties file "+propFile.getAbsolutePath());
      System.exit(2);
    }
    System.out.println("Reading properties from "+propFile.getAbsolutePath());
    Properties p = new Properties();
    
    try (FileReader r = new FileReader(propFile)) {
      p.load(r);
    }
    System.out.println(p);
    long started = System.nanoTime();
    int filesCreated = new GmailBackup(p).backup();
    System.out.println("Files created: "+filesCreated+". Total elapsed: "+formatElapsed(System.nanoTime() - started));
  }

  private static String formatElapsed(long nanos) {
    long ms = TimeUnit.NANOSECONDS.toMillis(nanos);
    return String.format("%d:%02d:%02d.%03d", ms/3600000, (ms/60000)%60, (ms/1000)%60, ms%1000);
  }

}
