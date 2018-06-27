/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import com.paypal.namenode.HSQLDriver;
import com.paypal.security.SecurityConfiguration;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.cache.SuggestionsEngine;
import org.apache.hadoop.hdfs.server.namenode.queries.FileTypeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.apache.hadoop.hdfs.server.namenode.queries.MemorySizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.SpaceSizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.TimeHistogram;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.TokenExtractor;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.GSetCollectionWrapper;
import org.apache.hadoop.util.GSetParallelWrapper;
import org.apache.hadoop.util.GSetSeperatorWrapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NNLoader {

  public static final Logger LOG = LoggerFactory.getLogger(NNLoader.class.getName());

  private final VersionInterface versionLoader;
  private final SuggestionsEngine suggestionsEngine;
  private final QueryEngine qEngine;

  private AtomicBoolean inited = new AtomicBoolean(false);
  private AtomicBoolean historical = new AtomicBoolean(false);
  private Configuration conf = null;
  private FSNamesystem namesystem = null;
  private HSQLDriver hsqlDriver = null;
  private Set<INode> all = null;
  private Map<INode, INode> files = null;
  private Map<INode, INode> dirs = null;
  private TokenExtractor tokenExtractor = null;


  public NNLoader() {
    versionLoader = new VersionContext();
    suggestionsEngine = new SuggestionsEngine();
    qEngine = new QueryEngine();
  }

  public TokenExtractor getTokenExtractor() {
    return tokenExtractor;
  }

  public HSQLDriver getEmbeddedHistoryDatabaseDriver() {
    return hsqlDriver;
  }

  public SuggestionsEngine getSuggestionsEngine() {
    return suggestionsEngine;
  }

  public QueryEngine getQueryEngine() { return qEngine; }

  public boolean isInit() {
    return inited.get();
  }

  public boolean isHistorical() {
    return historical.get();
  }

  public long getCurrentTxID() {
    if (namesystem == null) {
      return -1L;
    }
    return namesystem.getFSImage().lastAppliedTxId;
  }

  public String getAuthority() {
    if (conf == null) {
      return "test";
    }
    String authority =
        new Path(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY)).toUri().getAuthority();
    if (authority == null) {
      return "test";
    }
    return authority;
  }

  public void sendLoadingStatus(HttpServletResponse resp) throws IOException {
    String COUNT = "count";
    String ELAPSED_TIME = "elapsedTime";
    String FILE = "file";
    String NAME = "name";
    String DESC = "desc";
    String PERCENT_COMPLETE = "percentComplete";
    String PHASES = "phases";
    String SIZE = "size";
    String STATUS = "status";
    String STEPS = "steps";
    String TOTAL = "total";

    StartupProgressView view = NameNode.getStartupProgress().createView();
    JsonGenerator json =
        new JsonFactory().createJsonGenerator(resp.getWriter()).useDefaultPrettyPrinter();

    try {
      json.writeStartObject();
      json.writeNumberField(ELAPSED_TIME, view.getElapsedTime());
      json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete());
      json.writeArrayFieldStart(PHASES);

      for (Phase phase : view.getPhases()) {
        json.writeStartObject();
        json.writeStringField(NAME, phase.getName());
        json.writeStringField(DESC, phase.getDescription());
        json.writeStringField(STATUS, view.getStatus(phase).toString());
        json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase));
        json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase));
        writeStringFieldIfNotNull(json, FILE, view.getFile(phase));
        writeNumberFieldIfDefined(json, SIZE, view.getSize(phase));
        json.writeArrayFieldStart(STEPS);

        for (Step step : view.getSteps(phase)) {
          json.writeStartObject();
          StepType stepType = step.getType();
          if (stepType != null) {
            json.writeStringField(NAME, stepType.getName());
            json.writeStringField(DESC, stepType.getDescription());
          }
          json.writeNumberField(COUNT, view.getCount(phase, step));
          writeStringFieldIfNotNull(json, FILE, step.getFile());
          writeNumberFieldIfDefined(json, SIZE, step.getSize());
          json.writeNumberField(TOTAL, view.getTotal(phase, step));
          json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase, step));
          json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase, step));
          json.writeEndObject();
        }

        json.writeEndArray();
        json.writeEndObject();
      }

      json.writeEndArray();
      json.writeEndObject();
    } finally {
      IOUtils.closeStream(json);
    }
  }

  public void dumpINodeInDetail(String path, HttpServletResponse resp) throws IOException {
    versionLoader.dumpINodeInDetail(path, resp);
  }

  public void dumpConfig(HttpServletResponse resp) throws IOException {
    PrintWriter writer = resp.getWriter();
    try {
      conf.writeXml(writer);
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  public String getConfigValue(String key) throws IOException {
    return conf.get(key);
  }

  public void dumpLog(Integer charsLimitVar, HttpServletResponse resp) throws IOException {
    int charLimit = (charsLimitVar != null) ? charsLimitVar : 4000;
    LOG.info("Dumping last {} chars of logging to a client.", charLimit);
    long start = System.currentTimeMillis();
    PrintWriter writer = resp.getWriter();
    String logPath = System.getProperty("hadoop.log.dir", "/var/log/nn-analytics");
    String logFile = System.getProperty("hadoop.log.file", "nn-analytics.log");
    RandomAccessFile reader = new RandomAccessFile(logPath + "/" + logFile, "r");
    long startOffsetCalc = reader.length() - charLimit;
    long startOffset = (startOffsetCalc < 0) ? 0 : startOffsetCalc;
    reader.seek(startOffset);
    try {
      for (int charsRead = 0; charsRead < charLimit; charsRead++) {
        int charac = reader.read();
        writer.write(charac);
        writer.flush();
      }
    } finally {
      IOUtils.closeStream(reader);
      IOUtils.closeStream(writer);
      LOG.info("Closed response.");
    }
    long end = System.currentTimeMillis();
    LOG.info("Dumping the log response took {} ms.", (end - start));
  }


  public void saveNamespace() throws IOException {
    if (!isInit()) {
      throw new IllegalStateException("Namesystem is not initalized. Cannot saveNamespace.");
    }
    if (namesystem != null) {
      versionLoader.saveNamespace();
    } else {
      throw new IOException("Namesystem does not exist.");
    }
  }

  public void saveLegacyNamespace(String dir) throws IOException {
    if (!isInit()) {
      throw new IllegalStateException("Namesystem is not initalized. Cannot saveNamespace.");
    }
    if (namesystem != null) {
      versionLoader.saveLegacyOIVImage(dir);
    } else {
      throw new IOException("Namesystem does not exist.");
    }
  }

  @SuppressWarnings("unchecked") /* We do unchecked casting to extract GSets */
  public void load(GSet<INode, INodeWithAdditionalFields> preloadedInodes)
      throws InterruptedException, NoSuchFieldException, IllegalAccessException {
    /*
     * Configuration standard is: /etc/hadoop/conf.
     * Goal is to let configuration tell us where the FsImage and EditLogs are for loading.
     */

    conf = new Configuration();
    conf.addResource("hdfs-default.xml");
    conf.addResource("hdfs-site.xml");
    long start = System.currentTimeMillis();

    GSetParallelWrapper<INode, INodeWithAdditionalFields> gsetMap;
    if (preloadedInodes == null) {
      LOG.info("Setting: {} to: {}", DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);
      conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);

      LOG.info("Setting: {} to: {} ", DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, (-1));
      conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, -1);

      LOG.info("Setting: {} to: {}", DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY, false);
      conf.setBoolean(DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY, false);

      LOG.info(
          "Setting: {} to: /usr/local/nn-analytics/dfs/name",
          DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, "/usr/local/nn-analytics/dfs/name");

      String nameserviceId = DFSUtil.getOnlyNameServiceIdOrNull(conf);
      nameserviceId =
          (nameserviceId == null) ? conf.get(DFSConfigKeys.DFS_NAMESERVICE_ID) : nameserviceId;
      if (nameserviceId == null || nameserviceId.isEmpty()) {
        /* Hack for 2.4.0 support. attempt to override with internal nameservices. */
        nameserviceId = conf.get("dfs.internal.nameservices");

        LOG.info("Setting: {} to: {}", DFSConfigKeys.DFS_NAMESERVICE_ID, nameserviceId);
        conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, nameserviceId);
      }

      UserGroupInformation.setConfiguration(conf);
      reloadKeytab();

      LOG.info("Loading with configuration: {}", conf.toString());
      LOG.info(
          "FileSystem seen as: {}", conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
      LOG.info("Loading image from: {}", conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY));
      long start1 = System.currentTimeMillis();
      try {
        namesystem = FSNamesystem.loadFromDisk(conf);
        namesystem.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
      } catch (IOException e) {
        LOG.info("Failed to load namesystem: {}", e);
        return;
      }
      long end1 = System.currentTimeMillis();
      LOG.info("FSImage loaded in: {} ms.", (end1 - start1));
      LOG.info("Loaded in {} Inodes", namesystem.getFilesTotal());

      namesystem.writeLock();
      tokenExtractor = new TokenExtractor(namesystem.dtSecretManager, namesystem);
      FSDirectory fsDirectory = namesystem.getFSDirectory();
      INodeMap iNodeMap = fsDirectory.getINodeMap();
      Field mapField = iNodeMap.getClass().getDeclaredField("map");
      mapField.setAccessible(true);
      gsetMap =
          new GSetParallelWrapper((GSet<INode, INodeWithAdditionalFields>) mapField.get(iNodeMap));
    } else {
      gsetMap = new GSetParallelWrapper(preloadedInodes);
      tokenExtractor = new TokenExtractor(null, null);
    }

    long s1 = System.currentTimeMillis();
    all = new GSetCollectionWrapper(gsetMap);
    files =
        StreamSupport.stream(gsetMap.spliterator(), true)
            .filter(INode::isFile)
            .collect(Collectors.toConcurrentMap(node -> node, node -> node));
    dirs =
        StreamSupport.stream(gsetMap.spliterator(), true)
            .filter(INode::isDirectory)
            .collect(Collectors.toConcurrentMap(node -> node, node -> node));
    long e1 = System.currentTimeMillis();
    LOG.info("Filtering {} files and {} dirs took: {} ms.", files.size(), dirs.size(), (e1 - s1));

    if (preloadedInodes == null) {
      // Start tailing and updating security credentials threads.
      try {
        FSDirectory fsDirectory = namesystem.getFSDirectory();
        INodeMap iNodeMap = fsDirectory.getINodeMap();
        Field mapField = iNodeMap.getClass().getDeclaredField("map");
        mapField.setAccessible(true);
        GSet<INode, INodeWithAdditionalFields> newGSet =
            new GSetSeperatorWrapper(gsetMap, files, dirs);
        mapField.set(iNodeMap, newGSet);
        namesystem.writeUnlock();

        namesystem.startStandbyServices(conf);
        versionLoader.setNamesystem(namesystem);
      } catch (Throwable e) {
        LOG.info("ERROR: Failed to start EditLogTailer: {}", e);
      }
    }

    long end = System.currentTimeMillis();
    LOG.info("NNLoader bootstrap'd in: {} ms.", (end - start));
    inited.set(true);
    qEngine.setInited(true);
  }

  private void writeNumberFieldIfDefined(JsonGenerator json, String key, Long value)
      throws IOException {
    if (value != Long.MIN_VALUE) {
      json.writeNumberField(key, value);
    }
  }

  private void writeStringFieldIfNotNull(JsonGenerator json, String key, String value)
      throws IOException {
    if (value != null) {
      json.writeStringField(key, value);
    }
  }

  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }

  Configuration getConfiguration() {
    return conf;
  }

  private void reloadKeytab() {
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        SecurityUtil.login(
            conf,
            DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
            DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
            InetAddress.getLocalHost().getCanonicalHostName());
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void clear() {
    if (namesystem != null) {
      try {
        namesystem.shutdown();
      } finally {
        namesystem = null;
      }
    }
    if (all != null) {
      all.clear();
    }
    if (files != null) {
      files.clear();
    }
    if (dirs != null) {
      dirs.clear();
    }
    inited.set(false);
    qEngine.setInited(false);
  }


  public void namesystemWriteLock(Boolean useLock) {
    if (useLock != null && useLock && namesystem != null) {
      namesystem.writeLock();
    }
  }

  public void namesystemWriteUnlock(Boolean useLock) {
    if (useLock != null && useLock && namesystem != null) {
      namesystem.writeUnlock();
    }
  }

  public void initReloadThreads(ExecutorService internalService, SecurityConfiguration conf) {
    Future<Void> reload =
        internalService.submit(
            () -> {
              while (true) {
                try {
                  suggestionsEngine.reloadSuggestions(this);
                } catch (Throwable e) {
                  LOG.info("Suggestion reload failed: {}", e);
                  for (StackTraceElement element : e.getStackTrace()) {
                    LOG.info(element.toString());
                  }
                }
                try {
                  Thread.sleep(conf.getSuggestionsReloadSleepMs());
                } catch (InterruptedException ignored) {
                }
              }
            });
    Future<Void> keytab =
        internalService.submit(
            () -> {
              while (true) {
                // Reload Keytab every 10 minutes.
                try {
                  Thread.sleep(10 * 60 * 1000L);
                } catch (InterruptedException ignored) {
                }
                reloadKeytab();
              }
            });
    if (reload.isDone()) {
      LOG.error("Suggestion reload service exited; suggestions will not update.");
    }
    if (keytab.isDone()) {
      LOG.error("Keytab reload service exited; keytab will expire.");
    }
  }

  public void initHistoryRecorder(
      HSQLDriver hsqlDriver, SecurityConfiguration conf, boolean isEnabled) throws SQLException {
    if (isEnabled && hsqlDriver != null) {
      this.hsqlDriver = hsqlDriver;
      hsqlDriver.startDatabase(conf);
      hsqlDriver.createTable();
      historical.set(true);
    }
  }
}
