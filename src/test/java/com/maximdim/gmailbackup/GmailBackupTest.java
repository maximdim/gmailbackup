package com.maximdim.gmailbackup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GmailBackupTest {
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String PRECISE = "yyyy-MM-dd'T'HH:mm:ss";

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final SimpleDateFormat df = new SimpleDateFormat(PRECISE);
  private final SimpleDateFormat dfLegacy = new SimpleDateFormat("yyyy-MM-dd");
  private PrintStream realOut;

  /** The class logs its configuration and every save, which would bury the test output. */
  @Before
  public void quiet() {
    this.realOut = System.out;
    System.setOut(new PrintStream(new ByteArrayOutputStream()));
  }

  @After
  public void unquiet() {
    System.setOut(this.realOut);
  }

  private GmailBackup backup(File timestampFile, String users) {
    Properties p = new Properties();
    p.setProperty("serviceAccountId", "test");
    p.setProperty("serviceAccountPkFile", "/dev/null");
    p.setProperty("domain", "example.com");
    p.setProperty("timestampFile", timestampFile.getAbsolutePath());
    p.setProperty("users", users);
    p.setProperty("ignoreFrom", "spam@example.com");
    p.setProperty("dataDir", timestampFile.getParentFile().getAbsolutePath());
    p.setProperty("oldestDate", "2020-01-01");
    return new GmailBackup(p);
  }

  // --- resume timestamp -----------------------------------------------------------------------
  // The timestamp used to be stored rounded back to the start of the previous day. A user with
  // more than maxPerRun messages in one calendar day could then never advance past that day.

  @Test
  public void legacyDateOnlyTimestampsStillParse() throws Exception {
    Date d = GmailBackup.parseTimestamp(this.df, this.dfLegacy, "2026-08-11");
    assertEquals("2026-08-11T00:00:00", this.df.format(d));
  }

  @Test
  public void preciseTimestampsRoundTrip() throws Exception {
    Date d = GmailBackup.parseTimestamp(this.df, this.dfLegacy, "2026-08-11T14:23:05");
    assertEquals("2026-08-11T14:23:05", this.df.format(d));
    assertEquals("2026-08-11T14:23:05",
        this.df.format(GmailBackup.parseTimestamp(this.df, this.dfLegacy, this.df.format(d))));
  }

  @Test
  public void savedLineStaysReadableByTheSplitOnEqualsReader() throws Exception {
    String line = "nku=" + this.df.format(this.df.parse("2026-08-11T14:23:05"));
    assertEquals(2, line.split("=").length);
  }

  /** Whatever the time of day, the server side bound is the start of the previous day. */
  @Test
  public void searchWindowStartsADayBeforeRegardlessOfTime() throws Exception {
    assertEquals("2026-08-10T00:00:00",
        this.df.format(GmailBackup.searchWindowStart(this.df.parse("2026-08-11T14:23:05"))));
    assertEquals("2026-08-10T00:00:00",
        this.df.format(GmailBackup.searchWindowStart(this.df.parse("2026-08-11T00:00:00"))));
    assertEquals("2026-08-10T00:00:00",
        this.df.format(GmailBackup.searchWindowStart(this.df.parse("2026-08-11T23:59:59"))));
  }

  /**
   * The livelock itself: two runs cut off mid-day by maxPerRun have to resume at different points,
   * while still searching from the same safe day-granular bound.
   */
  @Test
  public void twoRunsWithinOneDayMakeProgress() throws Exception {
    Date endOfRun1 = this.df.parse("2026-07-06T09:15:00");
    Date endOfRun2 = this.df.parse("2026-07-06T13:40:00");

    assertTrue("second run must resume past the first", endOfRun2.after(endOfRun1));
    assertEquals("2026-07-05T00:00:00", this.df.format(GmailBackup.searchWindowStart(endOfRun1)));
    assertEquals("2026-07-05T00:00:00", this.df.format(GmailBackup.searchWindowStart(endOfRun2)));

    // the local cutoff drops what run 1 already did, but keeps the boundary message itself, so a
    // cut through same-second messages re-examines them (EXISTS) rather than skipping them
    assertTrue(this.df.parse("2026-07-06T09:14:59").before(endOfRun1));
    assertFalse(endOfRun1.before(endOfRun1));
  }

  // --- timestamp file -------------------------------------------------------------------------

  @Test
  public void saveReplacesTheFileRatherThanTruncatingIt() throws Exception {
    File ts = this.tmp.newFile("users.properties");
    Files.write(ts.toPath(), "alice=2020-01-01\nbob=2020-01-01\n".getBytes(UTF8));
    Object inodeBefore = Files.readAttributes(ts.toPath(), BasicFileAttributes.class).fileKey();

    GmailBackup gb = backup(ts, "alice,bob");
    Map<String, Date> data = new LinkedHashMap<String, Date>();
    data.put("alice", this.df.parse("2026-07-06T09:15:00"));
    data.put("bob", this.df.parse("2026-08-11T23:59:59"));
    gb.saveTimestamp(data, ts);

    assertEquals("alice=2026-07-06T09:15:00\nbob=2026-08-11T23:59:59\n",
        new String(Files.readAllBytes(ts.toPath()), UTF8));
    assertFalse("temp file must not be left behind",
        new File(ts.getParentFile(), "users.properties.tmp").exists());
    // a rename, not a write through the existing file - that is what makes a crash safe
    Object inodeAfter = Files.readAttributes(ts.toPath(), BasicFileAttributes.class).fileKey();
    assertFalse(String.valueOf(inodeBefore).equals(String.valueOf(inodeAfter)));
  }

  @Test
  public void whatIsSavedIsWhatTheNextRunReadsBack() throws Exception {
    File ts = this.tmp.newFile("users.properties");
    GmailBackup gb = backup(ts, "alice,bob");
    Map<String, Date> data = new LinkedHashMap<String, Date>();
    data.put("alice", this.df.parse("2026-07-06T09:15:00"));
    data.put("bob", this.df.parse("2026-08-11T23:59:59"));
    gb.saveTimestamp(data, ts);

    Map<String, Date> reloaded = gb.loadTimestamp(ts, this.df.parse("2020-01-01T00:00:00"));
    assertEquals("2026-07-06T09:15:00", this.df.format(reloaded.get("alice")));
    assertEquals("2026-08-11T23:59:59", this.df.format(reloaded.get("bob")));
  }

  @Test
  public void usersNoLongerConfiguredAreDroppedAndMissingOnesDefaulted() throws Exception {
    File ts = this.tmp.newFile("users.properties");
    Files.write(ts.toPath(), "alice=2026-08-11T10:00:00\ngone=2026-08-11T10:00:00\n".getBytes(UTF8));

    GmailBackup gb = backup(ts, "alice,bob");
    Map<String, Date> loaded = gb.loadTimestamp(ts, this.df.parse("2020-01-01T00:00:00"));
    assertEquals("2026-08-11T10:00:00", this.df.format(loaded.get("alice")));
    assertEquals("2020-01-01T00:00:00", this.df.format(loaded.get("bob")));
    assertNull("dropped users must not come back", loaded.get("gone"));
  }

  /** A failed write must never cost us the timestamps we already had. */
  @Test
  public void aFailedSaveLeavesThePreviousFileIntact() throws Exception {
    File dir = this.tmp.newFolder("readonly");
    File ts = new File(dir, "users.properties");
    Files.write(ts.toPath(), "alice=2026-01-01T00:00:00\n".getBytes(UTF8));
    GmailBackup gb = backup(ts, "alice");

    Assume.assumeFalse("root ignores file permissions", "root".equals(System.getProperty("user.name")));
    assertTrue("test needs a non writable directory", dir.setWritable(false));
    PrintStream realErr = System.err;
    System.setErr(new PrintStream(new ByteArrayOutputStream()));
    try {
      Map<String, Date> data = new LinkedHashMap<String, Date>();
      data.put("alice", this.df.parse("2026-08-11T23:59:59"));
      gb.saveTimestamp(data, ts);
    }
    finally {
      System.setErr(realErr);
      dir.setWritable(true);
    }
    assertEquals("alice=2026-01-01T00:00:00\n", new String(Files.readAllBytes(ts.toPath()), UTF8));
  }

  // --- file name hash -------------------------------------------------------------------------

  /**
   * md5Hex replaced commons-codec DigestUtils.md5Hex(). The hash is part of every backed up file
   * name, so any difference would re-save every message already on disk under a new name.
   * commons-codec is kept at test scope purely so this comparison stays possible.
   */
  @Test
  public void md5HexMatchesCommonsCodecExactly() {
    String[] inputs = {
        "",
        "a",
        "abc",
        "maxim@maximdim.comHello there",
        "\"Some One\" <some.one@example.com>Re: [ticket] update",
        "Ünïcödé sübjéct with accents",
        "日本語の件名", // non latin, to pin the UTF-8 encoding
        "emoji 📧 subject",
        "trailing space ",
        "0000",
    };
    for (String in : inputs) {
      assertEquals("hash differs for [" + in + "]",
          DigestUtils.md5Hex(in), GmailBackup.md5Hex(in));
    }
  }

  /** Every byte value matters: a sign extension slip would only show up on some inputs. */
  @Test
  public void md5HexMatchesCommonsCodecAcrossManyInputs() {
    for (int i = 0; i < 2000; i++) {
      String in = "from" + i + "@example.com" + (char) (i % 0xd800) + "subject " + i;
      assertEquals(DigestUtils.md5Hex(in), GmailBackup.md5Hex(in));
    }
  }

  @Test
  public void md5HexIsLowercaseHexOfTheRightLength() {
    String hash = GmailBackup.md5Hex("anything");
    assertEquals(32, hash.length());
    assertTrue("expected lowercase hex, got " + hash, hash.matches("[0-9a-f]{32}"));
  }

  // --- concurrency ----------------------------------------------------------------------------

  /**
   * Users are backed up in parallel, so they share userTimestamps and the file it is written to.
   * A reader racing the writers must never see a torn or empty file.
   */
  @Test
  public void parallelSavesNeverProduceATornFile() throws Exception {
    final File ts = this.tmp.newFile("users.properties");
    final String[] users = {"alice", "bob", "carol", "dave", "erin", "frank"};
    final GmailBackup gb = backup(ts, String.join(",", users));

    final Map<String, Date> shared = new LinkedHashMap<String, Date>();
    for (String u : users) {
      shared.put(u, new Date(1750000000000L));
    }
    gb.saveTimestamp(shared, ts);

    final AtomicReference<Throwable> boom = new AtomicReference<Throwable>();
    final CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(users.length + 1);
    List<Future<?>> futures = new ArrayList<Future<?>>();

    for (final String u : users) {
      futures.add(pool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            start.await();
            for (int i = 0; i < 150; i++) {
              synchronized (shared) { // the same discipline backupUser() uses
                shared.put(u, new Date(1750000000000L + i * 1000L));
              }
              gb.saveTimestamp(shared, ts);
            }
          }
          catch (Throwable t) {
            boom.compareAndSet(null, t);
          }
        }
      }));
    }

    futures.add(pool.submit(new Runnable() {
      @Override
      public void run() {
        try {
          start.await();
          SimpleDateFormat reader = new SimpleDateFormat(PRECISE);
          for (int i = 0; i < 3000; i++) {
            List<String> lines = Files.readAllLines(ts.toPath(), UTF8);
            if (lines.size() != users.length) {
              throw new IllegalStateException("torn read: " + lines.size() + " lines " + lines);
            }
            for (String line : lines) {
              String[] ss = line.split("=");
              if (ss.length != 2) {
                throw new IllegalStateException("torn line [" + line + "]");
              }
              reader.parse(ss[1]);
            }
          }
        }
        catch (Throwable t) {
          boom.compareAndSet(null, t);
        }
      }
    }));

    start.countDown();
    for (Future<?> f : futures) {
      f.get();
    }
    pool.shutdown();

    assertNull(String.valueOf(boom.get()), boom.get());
    assertFalse(new File(ts.getParentFile(), "users.properties.tmp").exists());

    List<String> finalLines = Files.readAllLines(ts.toPath(), UTF8);
    assertEquals(users.length, finalLines.size());
    TreeSet<String> seen = new TreeSet<String>();
    for (String line : finalLines) {
      seen.add(line.split("=")[0]);
    }
    assertEquals(new TreeSet<String>(Arrays.asList(users)), seen);
  }
}
