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
package com.trino.on.yarn.util;

import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import com.trino.on.yarn.constant.Constants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static com.trino.on.yarn.constant.Constants.S_3_A;

/**
 * YarnHelper
 */
public class YarnHelper {

    private static final String YARN_APP_ID_PARSE_REGEX = "(application_\\d{13}_\\d+)";

    private static final Log LOG = LogFactory.getLog(YarnHelper.class);

    /**
     * 获取yarn app id
     *
     * @param content
     */
    public static String getYarnAppId(String content) {
        return ReUtil.extractMulti(YARN_APP_ID_PARSE_REGEX, content, "$1");
    }

    public static String buildClassPathEnv(Configuration conf) {
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/log4j.properties")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CLIENT_CONF_DIR")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$HADOOP_CONF_DIR")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$JAVA_HOME/lib/tools.jar")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/conf/")
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("$PWD/")
                .append(Constants.JAR_FILE_LINKEDNAME).append("/*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        return classPathEnv.toString();
    }

    public static void addFrameworkToDistributedCache(String javaPathInHdfs, Map<String, LocalResource> localResources, Configuration conf) throws IOException {
        URI uri = getUri(javaPathInHdfs);
        Path path = new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
        FileSystem fs = path.getFileSystem(conf);
        Path frameworkPath = fs.makeQualified(new Path(uri.getScheme(), uri.getAuthority(), uri.getPath()));

        FileContext fc = FileContext.getFileContext(frameworkPath.toUri(), conf);
        frameworkPath = fc.resolvePath(frameworkPath);
        uri = frameworkPath.toUri();
        try {
            uri = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, Constants.JAR_FILE_LINKEDNAME);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        FileStatus scFileStatus = fs.getFileStatus(frameworkPath);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(uri),
                LocalResourceType.ARCHIVE, LocalResourceVisibility.PRIVATE,
                scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(Constants.JAR_FILE_LINKEDNAME, scRsrc);
    }

    private static URI getUri(String javaPathInHdfs) {
        URI uri;
        try {
            uri = new URI(javaPathInHdfs);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Unable to parse '" + javaPathInHdfs + "' as a URI.");
        }
        return uri;
    }

    public static Path addToLocalResources(Configuration conf, String path, String name, Map<String, LocalResource> localResources) throws IOException, URISyntaxException {
        FileSystem fs = getFileSystem(conf, path);
        return addToLocalResources(fs, path, name, localResources);
    }

    public static FileSystem getFileSystem(Configuration conf, String path) throws IOException, URISyntaxException {
        FileSystem fs;
        if (StrUtil.startWith(path, S_3_A)) {
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            fs = FileSystem.get(new URI(path), conf);
        } else {
            fs = FileSystem.get(conf);
        }
        return fs;
    }

    public static Path addToLocalResources(FileSystem fs, String path, String name, Map<String, LocalResource> localResources) throws IOException {
        Path dst = fs.makeQualified(new Path(path));
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.ARCHIVE, LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(name, scRsrc);
        return dst;
    }

    public static Path addToLocalResources(String appName, FileSystem fs, String fileSrcPath,
                                           String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                           String resources) throws IOException {
        Path dst = getPath(appName, fs, fileSrcPath, fileDstPath, appId, resources);
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
        return dst;
    }

    public static Path getPath(String appName, FileSystem fs, String fileSrcPath, String fileDstPath, String appId, String resources) throws IOException {
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        return dst;
    }


    public static String put(String appName, FileSystem fs, String fileSrcPath, String fileDstPath, String appId) throws IOException {
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        return dst.toUri().getRawPath();
    }
}
