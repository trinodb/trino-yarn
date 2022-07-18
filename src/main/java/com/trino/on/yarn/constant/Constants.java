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
package com.trino.on.yarn.constant;

/**
 * Constants
 */
public class Constants {

    public static final String SHELL_ARGS_PATH = "shellArgs";

    public static final String JAVA_OPTS_PATH = "javaOpts";

    public static final String JAR_FILE_LINKEDNAME = "jar";

    public static final String APP_MASTER_JAR_PATH = "AppMaster.jar";

    public static final String JAR_FILE_PATH = "JAR_FILE_PATH";

    public static final String LOG_4_J_PATH = "log4j.properties";

    public static final String DATAX = "datax";

    public static final String DATAX_HOME = "/" + DATAX + "/" + DATAX + "/";

    public static final String DATAX_JOB = "datax.job";

    public static final String DATAX_SCRIPT_PYTHON = "#!/bin/bash\n/usr/bin/python %s/bin/datax.py --jvm=\"-Xms%dm -Xmx%dm\" %s";

}
