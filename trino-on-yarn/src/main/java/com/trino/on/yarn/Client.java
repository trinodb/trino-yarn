/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trino.on.yarn;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.constant.Constants;
import com.trino.on.yarn.constant.RunType;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.server.ClientServer;
import com.trino.on.yarn.server.Server;
import com.trino.on.yarn.util.Log4jPropertyHelper;
import com.trino.on.yarn.util.YarnHelper;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static com.trino.on.yarn.constant.Constants.*;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

    private static final Log LOG = LogFactory.getLog(Client.class);

    // Configuration
    private final Configuration conf;
    private final YarnClient yarnClient;
    // Main class to invoke application master
    private final String appMasterMainClass;
    // Env variables to be setup for the shell command
    private final Map<String, String> shellEnv = new HashMap<>();
    // Shell Command Container priority
    private final int shellCmdPriority = 0;
    // Start time for client
    private final long clientStartTime = System.currentTimeMillis();
    // Command line options
    private final Options opts;
    // Debug flag
    boolean debugFlag = false;
    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "";
    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 10;
    // Amt. of virtual core resource to request for to run the App Master
    private int amVCores = 1;
    // Application master jar file
    private String appMasterJar = "";
    private String[] shellArgs = new String[]{};
    private String[] javaOpts = new String[]{};
    // Amt of memory to request for container in which shell script will be executed
    private int containerMemory = 10;
    // Amt. of virtual cores to request for container in which shell script will be executed
    private int containerVirtualCores = 1;
    // No. of containers in which the shell script needs to be executed
    private int numContainers = 1;
    // log4j.properties file
    // if available, add to local resources and set into classpath
    private String log4jPropFile = "";
    // Timeout threshold for client. Kill app after time interval expires.
    // -1 means no timeout so that the application will not be killed after timeout,
    // in other words, long time running job will be kept running.
    private long clientTimeout = -1;
    // flag to indicate whether to keep containers across application attempts.
    private boolean keepContainers = false;
    private int memoryOverhead = 50;

    private JobInfo jobInfo;

    private String run;

    private SimpleServer simpleServer;

    private ApplicationId applicationId;
    /*
     * private String dataxJob = "";
     *
     * private String dataxHomeArchivePath = "";
     */


    /**
     *
     */
    public Client(Configuration conf) {
        this(ApplicationMaster.class.getName(), conf);
    }

    Client(String appMasterMainClass, Configuration conf) {
        this.conf = conf;
        // set am retry to a lot of times
        conf.set("yarn.resourcemanager.am.max-attempts", "99");
        this.appMasterMainClass = appMasterMainClass;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - DistributedShell");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("master_vcores", true, "Amount of virtual cores to be requested to run the application master");
        opts.addOption("memory_overhead", true, "Amount of memory overhead in MB for application master and container");
        opts.addOption("jar_path", true, "Jar file containing the application master in local file system");
        opts.addOption("datax_job", true, "Jar file containing the application master in HDFS");
        opts.addOption("datax_home_hdfs", true, "Jar file containing the application master in HDFS");
        opts.addOption("shell_args", true, "Command line args for the shell script."
                + "Multiple args can be separated by empty space.");
        opts.addOption("java_opts", true, "Java opts for container");
        opts.getOption("shell_args").setArgs(Option.UNLIMITED_VALUES);
        opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("keep_containers_across_application_attempts", false,
                "Flag to indicate whether to keep containers across application attempts."
                        + " If the flag is true, running containers will not be killed when"
                        + " application attempt fails and these containers will be retrieved by"
                        + " the new application attempt ");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
        opts.addOption("run_type", true, "run_type is yarn-per or yarn-session");
        opts.addOption("job_info", true, "******json*******");
    }

    /**
     *
     */
    public Client() throws Exception {
        this(new YarnConfiguration());
    }

    /**
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            Client client = new Client();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running CLient", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.error("Application failed");
        System.exit(2);
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * Parse command line options
     *
     * @param args Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {
        CommandLineParser gnuParser = new GnuParser();
        CommandLine cliParser = gnuParser.parse(opts, args);
        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        if (cliParser.hasOption("log_properties")) {
            String log4jPath = cliParser.getOptionValue("log_properties");
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(Client.class, log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }

        if (cliParser.hasOption("keep_containers_across_application_attempts")) {
            LOG.info("keep_containers_across_application_attempts");
            keepContainers = true;
        }

        appName = cliParser.getOptionValue("appname", "AppOnYarnDemo");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "128"));
        amVCores = Integer.parseInt(cliParser.getOptionValue("master_vcores", "1"));

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }
        if (amVCores < 0) {
            throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
                    + " Specified virtual cores=" + amVCores);
        }

        if (!cliParser.hasOption("jar_path")) {
            throw new IllegalArgumentException("No jar_path file path specified for application master");
        }
        appMasterJar = cliParser.getOptionValue("jar_path");

        if (!cliParser.hasOption("run_type") || StrUtil.isBlank(cliParser.getOptionValue("run_type"))) {
            throw new IllegalArgumentException("run_type isBlank");
        } else {
            run = cliParser.getOptionValue("run_type");
            if (RunType.YARN_PER.getName().equalsIgnoreCase(run) || RunType.YARN_SESSION.getName().equalsIgnoreCase(run)) {
            } else {
                throw new IllegalArgumentException("run_type isBlank/run_type is yarn-per or yarn-session,run_type:" + run);
            }
        }

        if (cliParser.hasOption("shell_args")) {
            shellArgs = cliParser.getOptionValues("shell_args");
        }
        if (cliParser.hasOption("java_opts")) {
            javaOpts = cliParser.getOptionValues("java_opts");
        }

        if (cliParser.hasOption("shell_env")) {
            String[] envs = cliParser.getOptionValues("shell_env");
            for (String env : envs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        memoryOverhead = Integer.parseInt(cliParser.getOptionValue("memory_overhead", "2"));

        if (containerMemory < 0 || containerVirtualCores < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory/vcores specified,"
                    + " exiting."
                    + " Specified containerMemory=" + containerMemory
                    + ", containerVirtualCores=" + containerVirtualCores
                    + ", numContainer=" + numContainers);
        }

        clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "-1"));

        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        if (!cliParser.hasOption("job_info")) {
            throw new IllegalArgumentException("job_info isBlank");
        }
        String jobInfoPath = cliParser.getOptionValue("job_info");
        String jobInfoStr = FileUtil.readUtf8String(jobInfoPath);

        if (StrUtil.isNotBlank(jobInfoStr) && JSONUtil.isTypeJSONObject(jobInfoStr)) {
            jobInfo = JSONUtil.toBean(jobInfoStr, JobInfo.class);
            if (jobInfo == null) {
                throw new IllegalArgumentException("job_info");
            }
        } else
            throw new IllegalArgumentException("job_info isBlank/is not JSONObject");

        LOG.warn("jobInfo:" + jobInfo);

        if (StrUtil.isNotBlank(jobInfo.getUser())) {
            System.setProperty("HADOOP_USER_NAME", jobInfo.getUser());
        }

        simpleServer = ClientServer.initClient();
        InetSocketAddress inetSocketAddress = simpleServer.getAddress();
        jobInfo.setPort(inetSocketAddress.getPort());
        jobInfo.setIp(Server.ip());
        jobInfo.setRunType(run);
        jobInfo.setNumTotalContainers(numContainers);

        return true;
    }

    /**
     * Main run function for the client
     *
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public boolean run() throws IOException, YarnException {

        LOG.info("Running Client");
        yarnClient.start();

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM"
                + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
                NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId()
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info"
                + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount=" + queueInfo.getApplications().size()
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue"
                        + ", queueName=" + aclInfo.getQueueName()
                        + ", userAcl=" + userAcl.name());
            }
        }

        // Get a new application id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        //  get min/max resource capabilities from RM and change memory ask if needed
        // If we do not have min/max, we may not be able to correctly request
        // the required resources from the RM for the app master
        // Memory ask has to be a multiple of min and less than max.
        // Dump out information about cluster capability as seen by the resource manager
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (amMemory + memoryOverhead > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem - memoryOverhead;
        }

        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

        if (amVCores > maxVCores) {
            LOG.info("AM virtual cores specified above max threshold of cluster. "
                    + "Using max value." + ", specified=" + amVCores
                    + ", max=" + maxVCores);
            amVCores = maxVCores;
        }

        // set the application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        if (jobInfo.getRunType().equalsIgnoreCase(RunType.YARN_PER.getName())) {
            appContext.setApplicationType(RunType.YARN_PER.getCode());
        } else if (jobInfo.getRunType().equalsIgnoreCase(RunType.YARN_SESSION.getName())) {
            String session = RunType.YARN_SESSION.getCode() + "-";
            if (StrUtil.startWith(jobInfo.getCatalog(), S3)) {
                session = session + S3.toLowerCase(Locale.ROOT);
            } else {
                session = session + DFS.toLowerCase(Locale.ROOT);
            }
            appContext.setApplicationType(session);
        } else {
            appContext.setApplicationType("trino");
        }

        ApplicationId appId = appContext.getApplicationId();

        if (StrUtil.startWith(jobInfo.getCatalogHdfs(), S_3_A)) {
            String bucket = jobInfo.getCatalogHdfs().replace(S_3_A, "").split("/")[0];
            appContext.setApplicationTags(CollUtil.newHashSet(bucket));
        }

        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);

        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources
        Map<String, LocalResource> localResources = new HashMap<>();

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(conf);
        Path dst = YarnHelper.addToLocalResources(appName, fs, appMasterJar, Constants.APP_MASTER_JAR_PATH, appId.toString(), localResources, null);

        YarnHelper.addFrameworkToDistributedCache(dst.toUri().toString(), localResources, conf);

        // Set the log4j properties if needed
        if (!log4jPropFile.isEmpty()) {
            YarnHelper.addToLocalResources(appName, fs, log4jPropFile, Constants.LOG_4_J_PATH, appId.toString(), localResources, null);
        }

        if (shellArgs.length > 0) {
            YarnHelper.addToLocalResources(appName, fs, null, Constants.SHELL_ARGS_PATH, appId.toString(), localResources, ArrayUtil.join(shellArgs, " "));
        }

        if (javaOpts.length > 0) {
            YarnHelper.addToLocalResources(appName, fs, null, Constants.JAVA_OPTS_PATH, appId.toString(), localResources, ArrayUtil.join(javaOpts, " "));
        }


        String catalogHdfs = jobInfo.getCatalogHdfs();
        if (!jobInfo.isHdfsOrS3()) {
            if (FileUtil.isDirectory(catalogHdfs)) {
                String zip = ZipUtil.zip(catalogHdfs).getAbsolutePath();
                Path path = YarnHelper.getPath(appName, fs, zip, JAVA_TRINO_CATALOG_PATH, appId.toString(), null);
                catalogHdfs = path.toUri().getPath();
                jobInfo.setCatalog(catalogHdfs);
            }
        }

        try {
            YarnHelper.addToLocalResources(conf, catalogHdfs, Constants.JAVA_TRINO_CATALOG_PATH, localResources);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Set the necessary security tokens as needed
        // amContainer.setContainerTokens(containerToken);

        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<>();

        env.put("CLASSPATH", YarnHelper.buildClassPathEnv(conf));
        env.put(Constants.JAR_FILE_PATH, dst.toUri().toString());

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
                "log4j.properties");

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        env.put("CLASSPATH", classPathEnv.toString());

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<>(30);

        int amMemory = 128;
        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(System.getenv("JAVA_HOME") + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        vargs.add("--container_memory " + containerMemory);
        vargs.add("--container_vcores " + containerVirtualCores);
        vargs.add("--memory_overhead " + memoryOverhead);
        vargs.add("--num_containers " + numContainers);
        vargs.add("--priority " + shellCmdPriority);
        vargs.add("--master_memory " + this.amMemory);
        vargs.add("--job_info " + Base64.encode(jobInfo.toString()));

        for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
            vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
        }
        if (debugFlag) {
            vargs.add("--debug");
        }

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command);
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        // Set up resource type requirements
        // For now, both memory and vcores are supported, so we set memory and
        // vcores requirements
        Resource capability = Resource.newInstance(amMemory + memoryOverhead, amVCores);
        appContext.setResource(capability);

        // Service data is a binary blob that can be passed to the application
        // Not needed in this scenario
        // amContainer.setServiceData(serviceData);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            final Token<?>[] tokens =
                    fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        // what is the range for priority? how to decide?
        Priority pri = Priority.newInstance(amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
        // Ignore the response as either a valid response object is returned on success
        // or an exception thrown to denote some form of a failure
        LOG.info("Submitting application to ASM");

        yarnClient.submitApplication(appContext);

        // Try submitting the same request again
        // app submission failure?

        // Monitor the application
        applicationId = appId;
        LOG.warn("ApplicationId:" + appId.toString());
        return monitorApplication(appId);

    }

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     *
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {

        while (true) {
            // Check app status every 5 second.
            ThreadUtil.sleep(5000);

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {

                LOG.info("Application did not finish."
                        + " YarnState=" + state + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }

            if (clientTimeout > 0 && System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application");
                forceKillApplication(appId);
                return false;
            }

            if (Server.FINISH.equals(2)) {
                forceKillApplication(appId);
            }
        }

    }

    /**
     * Kill a submitted application by sending a call to the ASM
     *
     * @param appId Application Id to be killed.
     * @throws YarnException
     * @throws IOException
     */
    private void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
        // clarify whether multiple jobs with the same app id can be submitted and be running at
        // the same time.
        // If yes, can we kill a particular attempt only?

        // Response can be ignored as it is non-null on success or
        // throws an exception in case of failures
        yarnClient.killApplication(appId);
    }
}
