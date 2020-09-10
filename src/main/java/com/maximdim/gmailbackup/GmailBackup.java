package com.maximdim.gmailbackup;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.mail.Address;
import javax.mail.FetchProfile;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessageRemovedException;
import javax.mail.MessagingException;
import javax.mail.search.AndTerm;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.SearchTerm;

import org.apache.commons.codec.digest.DigestUtils;

import com.google.code.samples.oauth2.OAuth2Authenticator;
import com.sun.mail.imap.IMAPFolder;
import com.sun.mail.imap.IMAPStore;
import com.sun.mail.util.FolderClosedIOException;
import com.sun.mail.util.MessageRemovedIOException;

public class GmailBackup {
  private static final String USER_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
  private static final String HEADER_MESSAGE_ID = "Message-ID";
  private final String serviceAccountId;
  private final File serviceAccountPkFile;
  private final String domain;
  private final File timestampFile;
  // <User, Date>
  private final Map<String, Date> userTimestamps;

  private final List<String> users;
  private final Set<String> ignoreFrom;
  private int maxPerRun;
  private boolean zip;
  private boolean gzip;
  private int fetchWindowDays;
  
  // storage format:
  // dataDir/domain/year/month/day/user_timestamp.mail
  private final File dataDir;

  public GmailBackup(Properties p) {
    this.serviceAccountId = p.getProperty("serviceAccountId");
    this.serviceAccountPkFile = new File(p.getProperty("serviceAccountPkFile"));
    this.domain = p.getProperty("domain");
    this.timestampFile = new File(p.getProperty("timestampFile"));
    this.users = Arrays.asList(p.getProperty("users").split(","));
    this.ignoreFrom = new HashSet<>(Arrays.asList(p.getProperty("ignoreFrom").split(",")));
    this.maxPerRun = Integer.parseInt(p.getProperty("maxPerRun", "1000"));
    this.zip = Boolean.parseBoolean(p.getProperty("zip"));
    this.gzip = Boolean.parseBoolean(p.getProperty("gzip"));
    if (this.zip && this.gzip) {
      throw new IllegalStateException("Both zip and gzip compression specified. Choose one");
    }
    this.fetchWindowDays = Integer.parseInt(p.getProperty("fetchWindowDays", "30"));
    
    Date oldestDate = getDate(p.getProperty("oldestDate", "2012/01/01"), "yyyy-MM-dd");
    this.userTimestamps = loadTimestamp(this.timestampFile, oldestDate);
    
    this.dataDir = new File(p.getProperty("dataDir"));
  }

  private void backup() throws Exception {
    OAuth2Authenticator.initialize();

    for(String user: this.users) {
      try {
        System.out.println("### Backing up ["+user+"]");
        String email = user + "@" + this.domain;
        IMAPStore store = getStore(email);
        if (store == null) {
          System.out.println("Store is null, skip");
          continue;
        }
        
        UserMessagesIterator iterator = new UserMessagesIterator(store, this.userTimestamps.get(user), this.ignoreFrom, this.fetchWindowDays);
        int count = 0;
        while(iterator.hasNext() && count < this.maxPerRun) {
          try {
            Message message = iterator.next();
            File f = generateFileName(user, message);
            boolean fileExists = f.exists();
            if (!fileExists) {
              saveMessage(message, f);
            }
            // update stats
            this.userTimestamps.put(user, message.getReceivedDate());
            System.out.println(iterator.getStats() + " " + f.getAbsolutePath() + (fileExists ? ": EXISTS" : ""));
            count++;
            if (count % 100 == 0) {
              saveTimestamp(this.userTimestamps, this.timestampFile);
            }
          }
          catch (MessageRemovedIOException e) {
            System.err.println(e.getMessage());
          }
          catch(FolderClosedIOException e) {
            System.err.println(e.getMessage());
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
    }
    System.out.println("Done\n");
  }

  private File saveMessage(Message message, File f) throws Exception {
    if (!f.getParentFile().exists()) {
      f.getParentFile().mkdirs();
    }
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
    
    File file = new File(folder, sb.toString());
    return file;
  }
  
  private String getHash(Message m) throws MessagingException {
    String from = m.getFrom() != null && m.getFrom().length > 0? m.getFrom()[0].toString() : "";
    String subject = m.getSubject() != null ? m.getSubject() : "";
    String hash = DigestUtils.md5Hex(from+""+subject);
    // no need to be super long - the hash part is there just to avoid (infrequent) name collisions
    return hash.substring(0, 5);
  }
  
  private IMAPStore getStore(String email) throws Exception {
    String authToken = OAuth2Authenticator.getToken(this.serviceAccountPkFile, this.serviceAccountId, email);
    if (authToken == null) {
      System.out.println("authToken null!");
      return null;
    }
    System.out.println("authToken OK");

    IMAPStore store = OAuth2Authenticator.connectToImap("imap.gmail.com", 993, email, authToken, false);
    System.out.println("imapStore OK");
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
  private Map<String, Date> loadTimestamp(File f, Date defaultDate) {
    Map<String, Date> result = new HashMap<String, Date>();
    if (f.exists() && f.canRead()) {
      try (BufferedReader br = new BufferedReader(new FileReader(f))) {
        String line = null;
        SimpleDateFormat df = new SimpleDateFormat(USER_TIMESTAMP_FORMAT);
        while((line = br.readLine()) != null) {
          if (line.trim().length() == 0) {
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
              result.put(user, df.parse(ss[1]));
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
    System.out.println("Loading timestamps");
    for(Map.Entry<String, Date> me: result.entrySet()) {
      System.out.println(me.getKey()+"="+me.getValue());
    }
    return result;
  }
  
  static class UserMessagesIterator implements Iterator<Message> {
    private final int fetchWindowDays;
    private final List<Message> messages;
    private int index;

    public UserMessagesIterator(IMAPStore store, Date fetchFrom, Set<String> ignoreFrom, int fetchWindowDays) throws MessagingException {
      this.fetchWindowDays = fetchWindowDays;
      Set<String> drafts = getDraftMessageIds(store);
      this.messages = getMessages(store, fetchFrom, ignoreFrom, drafts);
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

    private Set<String> getDraftMessageIds(IMAPStore store) throws MessagingException {
      IMAPFolder folder = (IMAPFolder) store.getFolder("[Gmail]/Drafts");
      folder.open(Folder.READ_ONLY);
      System.out.println("imap folder open OK: " + folder.getName());
      int totalMessages = folder.getMessageCount();
      System.out.println("Draft messages: " + totalMessages);

      Set<String> result = new HashSet<>();
      for (Message m : folder.getMessages()) {
        String[] header = m.getHeader(HEADER_MESSAGE_ID);
        if (header != null) {
          result.addAll(Arrays.asList(header));
        }
      }
      folder.close(false);
      System.out.println("Draft ids: " + result.size());
      return result;
    }

    private boolean isDraft(Message m, Set<String> drafts) throws MessagingException {
      String[] headers = m.getHeader(HEADER_MESSAGE_ID);
      if (headers == null) {
        System.out.println("No id header in message");
        return false;
      }
      for (String mid : headers) {
        if (drafts.contains(mid)) {
          return true;
        }
      }
      return false;
    }

    private List<Message> getMessages(IMAPStore store, Date fetchFrom, Set<String> ignoreFrom, Set<String> drafts) throws MessagingException {
      IMAPFolder folder = (IMAPFolder)store.getFolder("[Gmail]/All Mail");
      folder.open(Folder.READ_ONLY);
      System.out.println("imap folder open OK: " + folder.getName());
      int totalMessages = folder.getMessageCount();
      System.out.println("Total messages: " + totalMessages);

      List<Message> result = new ArrayList<Message>();
      for(Message m: fetch(folder, fetchFrom)) {
        try {
          if (isDraft(m, drafts)) {
            System.out.println("Ignoring draft message: "+m.getSubject());
            continue;
          }
          if (m.getReceivedDate() == null) {
            System.out.println("Message received date is null: "+m.getSubject());
            continue;
          }
          if (m.getReceivedDate().before(fetchFrom)) {
            //System.out.println("Message date "+m.getReceivedDate()+" is before "+fetchFrom);
            continue;
          }
          Address[] addresses = m.getFrom();
          if (addresses.length == 0) {
            System.out.println("Ignoring email with empty from");
            continue;
          }
          String from = addresses[0].toString();
          if (ignoreFrom.contains(from.toLowerCase())) {
            //System.out.println("Ignoring email from "+from);
            continue;
          }
          result.add(m);
        }
        catch (MessageRemovedException e) {
          System.out.println("Message already removed: "+e.getMessage());
        }
      }
      System.out.println("Result filtered to: " + result.size());
      return result;
    }

    private Date getDateDaysFrom(Date from, int days) {
      Calendar c = Calendar.getInstance();
      c.setTime(from);
      c.add(Calendar.DAY_OF_YEAR, days);
      return c.getTime();
    }
    
    private Message[] fetch(IMAPFolder folder, Date fetchFrom) throws MessagingException {
      // Gmail seems to be returning strange result with ComparisonTerm.GE
      SearchTerm st = new ReceivedDateTerm(ComparisonTerm.GT, fetchFrom);
      System.out.println("Setting fetchFrom to "+fetchFrom);
      
      Date fetchTo = getDateDaysFrom(fetchFrom, this.fetchWindowDays);
      if (fetchTo.before(new Date())) {
        SearchTerm stTo = new ReceivedDateTerm(ComparisonTerm.LT, fetchTo);
        st = new AndTerm(st, stTo);
        System.out.println("Setting fetchTo to "+fetchTo);
      }
      
      // IMAP search command disregards time, only date is used
      Message[] messages = folder.search(st);
      //Message[] messages = folder.getMessages();
      System.out.println("Search returned: " + messages.length);
      
      if (messages.length == 0 && fetchTo.before(new Date())) { // our search window could be too much in the past, retry
        System.out.println("Retrying with fetchFrom: "+fetchTo);
        return fetch(folder, fetchTo);
      }
      
      FetchProfile fp = new FetchProfile();
      fp.add(FetchProfile.Item.ENVELOPE);
      folder.fetch(messages, fp);

      // messages returned from search not in order. Since we might not process all of them at once we need to sort
      Arrays.sort(messages, new Comparator<Message>() {
        @Override
        public int compare(Message m1, Message m2) {
          try {
            @SuppressWarnings("deprecation")
            Date old = new Date(2000,1,1);
            Date d1 = m1.getSentDate() != null ? m1.getSentDate() : old;
            Date d2 = m2.getSentDate() != null ? m2.getSentDate() : old;
            return d1.compareTo(d2);
          } 
          catch (MessagingException e) {
            throw new RuntimeException("Comparator error: "+e.getMessage(), e);
          }
        }
      });

      return messages;
    }
    
  }

  private void saveTimestamp(Map<String, Date> data, File f) {
    System.out.println("Saving timestamps");

    // sort file by date
    Map<Date, String> sortedData = new TreeMap<>();
    for (Map.Entry<String, Date> me: data.entrySet()) {
      sortedData.put(me.getValue(), me.getKey());
    }

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(f))) {
      SimpleDateFormat df = new SimpleDateFormat(USER_TIMESTAMP_FORMAT);
      for(Map.Entry<Date, String> me: sortedData.entrySet()) {
        String line = me.getValue()+"="+df.format(me.getKey());
        bw.write(line + "\n");
        System.out.println(line);
      }
      bw.flush();
    }
    catch (IOException e) {
      System.err.println("Error saving user timestamps to "+f.getAbsolutePath()+": "+e.getMessage());
    }
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
    new GmailBackup(p).backup();
  }

}
