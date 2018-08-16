/*******************************************************************************
 * * Copyright 2018 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.fabric.model;

import static java.lang.String.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.hibernate.engine.transaction.jta.platform.internal.SynchronizationRegistryBasedSynchronizationStrategy;
import org.hyperledger.fabric.sdk.helper.Utils;

import com.impetus.blkch.BlkchnException;

/**
 * Config allows for a global config of the toolkit. Central location for all
 * toolkit configuration defaults.
 */

public class Config {

    private static final Log logger = LogFactory.getLog(Config.class);

    private static final String PROPBASE = "config.";

    private String path;

    private static final String GOSSIPWAITTIME = PROPBASE + "GossipWaitTime";

    private static final String INVOKEWAITTIME = PROPBASE + "InvokeWaitTime";

    private static final String DEPLOYWAITTIME = PROPBASE + "DeployWaitTime";

    private static final String PROPOSALWAITTIME = PROPBASE + "ProposalWaitTime";

    private static final String ADMIN_NAME = "ADMIN_NAME";

    public static final String LOGGERLEVEL = "org.hyperledger.fabric.sdk.loglevel";

    private static final String ORGS = PROPBASE + "property.";

    private static final Pattern orgPat = Pattern.compile("^" + Pattern.quote(ORGS) + "([^\\.]+)\\.mspid$");

    private static final String CHAINCODE_CONFIG = PROPBASE + "chaincode.";

    private static final Pattern chaincodePat = Pattern.compile("^" + Pattern.quote(CHAINCODE_CONFIG)
            + "([^\\.]+)\\.peers$");

    private static final String BLOCKCHAINTLS = PROPBASE + "blockchain.tls";

    private final Properties sdkProperties = new Properties();

    private final boolean runningTLS;

    private final boolean runningFabricCATLS;

    private final boolean runningFabricTLS;

    private Org configOrg;

    private final Map<String, List<PeerInfo>> chaincodeMap = new HashMap<>();

    private Properties hyperledgerProperties = new Properties();

    private Properties dbProperties = new Properties();

    private String admin;

    public Config(String configPath) {
        this.path = configPath.endsWith("/") ? configPath : configPath + "/";

        try {

            /**
             * All the properties will be obtained from config.properties file
             */
            sdkProperties.load(new FileInputStream(path + "/config.properties"));
            defaultProperty(LOGGERLEVEL, "DEBUG");
            org.apache.log4j.Level setTo = null;
            setTo = org.apache.log4j.Level.DEBUG;
            org.apache.log4j.Logger.getLogger("org.hyperledger.fabric").setLevel(setTo);

        } catch (IOException e) {
            // if not there no worries just use defaults
            logger.warn("Failed to load any configuration");
        } finally {

            // Default values

            defaultProperty(GOSSIPWAITTIME, "5000");
            defaultProperty(INVOKEWAITTIME, "100000");
            defaultProperty(DEPLOYWAITTIME, "120000");
            defaultProperty(PROPOSALWAITTIME, "120000");

            defaultProperty(BLOCKCHAINTLS, null);
            defaultProperty(LOGGERLEVEL, "DEBUG");
            runningTLS = ((null != sdkProperties.getProperty(BLOCKCHAINTLS, null)) && false != Boolean
                    .parseBoolean(sdkProperties.getProperty(BLOCKCHAINTLS, null)));
            runningFabricCATLS = runningTLS;
            runningFabricTLS = runningTLS;

            int orgCount = 0;
            for (Map.Entry<Object, Object> x : sdkProperties.entrySet()) {
                final String key = x.getKey() + "";
                final String val = x.getValue() + "";

                if (key.startsWith(ORGS)) {

                    Matcher match = orgPat.matcher(key);

                    if (match.matches() && match.groupCount() == 1) {
                        orgCount++;
                        if (orgCount > 1) {
                            throw new BlkchnException("More than one organizations are specified in config.properties");
                        }
                        String orgName = match.group(1).trim();
                        configOrg = new Org(orgName, val.trim());

                    }
                } else if (key.startsWith(CHAINCODE_CONFIG)) {
                    Matcher match = chaincodePat.matcher(key);
                    if (match.matches() && match.groupCount() == 1) {
                        String chaincodeName = match.group(1).trim();
                        String peerNames = val.trim();
                        String[] ps = peerNames.split("[ \t]*,[ \t]*");
                        List<PeerInfo> peerInfos = new ArrayList<>();
                        for (String peer : ps) {
                            String[] nl = peer.split("[ \t]*@[ \t]*");
                            peerInfos.add(new PeerInfo(nl[0], grpcTLSify(nl[1]), getEndPointProperties("peer", nl[0])));
                        }
                        chaincodeMap.put(chaincodeName, peerInfos);
                    }
                }
            }

            final String orgName = configOrg.getName();

            String peerNames = sdkProperties.getProperty(ORGS + orgName + ".peer_locations");
            String[] ps = peerNames.split("[ \t]*,[ \t]*");
            for (String peer : ps) {
                String[] nl = peer.split("[ \t]*@[ \t]*");
                configOrg.addPeerLocation(nl[0], grpcTLSify(nl[1]));
            }

            final String domainName = sdkProperties.getProperty(ORGS + orgName + ".domname");

            configOrg.setDomainName(domainName);

            final String usrsAffilation = sdkProperties.getProperty(ORGS + orgName + ".users_affilation");

            configOrg.setUserAffilation(usrsAffilation);

            String ordererNames = sdkProperties.getProperty(ORGS + orgName + ".orderer_locations");
            ps = ordererNames.split("[ \t]*,[ \t]*");
            for (String peer : ps) {
                String[] nl = peer.split("[ \t]*@[ \t]*");
                configOrg.addOrdererLocation(nl[0], grpcTLSify(nl[1]));
            }

            String eventHubNames = sdkProperties.getProperty(ORGS + orgName + ".eventhub_locations");
            ps = eventHubNames.split("[ \t]*,[ \t]*");
            for (String peer : ps) {
                String[] nl = peer.split("[ \t]*@[ \t]*");
                configOrg.addEventHubLocation(nl[0], grpcTLSify(nl[1]));
            }

            configOrg.setCALocation(httpTLSify(sdkProperties.getProperty((ORGS + orgName + ".ca_location"))));

            String cert = path
                    + "artifacts/channel/crypto-config/peerOrganizations/DNAME/ca/ca.DNAME-cert.pem".replaceAll(
                            "DNAME", domainName);
            File cf = new File(cert);
            if (!cf.exists() || !cf.isFile()) {
                throw new RuntimeException(" missing cert file " + cf.getAbsolutePath());
            }
            Properties properties = new Properties();
            properties.setProperty("pemFile", cf.getAbsolutePath());

            properties.setProperty("allowAllHostNames", "true");

            configOrg.setCAProperties(properties);

        }

        try {
            hyperledgerProperties.load(new FileInputStream(path + "/hyperledger.properties"));
            admin = hyperledgerProperties.getProperty(ADMIN_NAME) != null ? hyperledgerProperties
                    .getProperty(ADMIN_NAME) : null;
        } catch (IOException e) {
            logger.info("hyperledger.properties file from location " + path + " not found");
            admin = null;
        }

        try {
            File file = new File(path + "/db.properties");
            if (file.exists()) {
                dbProperties.load(new FileInputStream(path + "/db.properties"));
            }
        } catch (IOException e) {
            logger.error("Error loading db.properties file from location " + path, e);
        }
    }

    public Properties getDbProperties() {
        return dbProperties;
    }

    public List<PeerInfo> getChaincodePeers(String chaincodeName) {
        return chaincodeMap.get(chaincodeName);
    }

    private String grpcTLSify(String location) {
        location = location.trim();
        Exception e = Utils.checkGrpcUrl(location);
        if (e != null) {
            throw new RuntimeException(String.format("Bad  parameters for grpc url %s", location), e);
        }
        return runningFabricTLS ? location.replaceFirst("^grpc://", "grpcs://") : location;

    }

    private String httpTLSify(String location) {
        location = location.trim();

        return runningFabricCATLS ? location.replaceFirst("^http://", "https://") : location;
    }

    public String getAdmin() {
        return admin;
    }

    /**
     * getProperty return back property for the given value.
     *
     * @param property
     * @return String value for the property
     */
    private String getProperty(String property) {

        String ret = sdkProperties.getProperty(property);

        if (null == ret) {
            logger.warn(String.format("No configuration value found for '%s'", property));
        }
        return ret;
    }

    private void defaultProperty(String key, String value) {

        String ret = System.getProperty(key);
        if (ret != null) {
            sdkProperties.put(key, ret);
        } else {
            String envKey = key.toUpperCase().replaceAll("\\.", "_");
            ret = System.getenv(envKey);
            if (null != ret) {
                sdkProperties.put(key, ret);
            } else {
                if (null == sdkProperties.getProperty(key) && value != null) {
                    sdkProperties.put(key, value);
                }

            }

        }
    }

    public int getTransactionWaitTime() {
        return Integer.parseInt(getProperty(INVOKEWAITTIME));
    }

    public int getDeployWaitTime() {
        return Integer.parseInt(getProperty(DEPLOYWAITTIME));
    }

    public int getGossipWaitTime() {
        return Integer.parseInt(getProperty(GOSSIPWAITTIME));
    }

    public long getProposalWaitTime() {
        return Integer.parseInt(getProperty(PROPOSALWAITTIME));
    }

    public Org getSampleOrg() {
        return configOrg;
    }

    public Properties getPeerProperties(String name) {

        return getEndPointProperties("peer", name);

    }

    public Properties getOrdererProperties(String name) {

        return getEndPointProperties("orderer", name);

    }

    private Properties getEndPointProperties(final String type, final String name) {

        final String domainName = getDomainName(name);

        File cert = Paths.get(getChannelPath(), "crypto-config/ordererOrganizations".replace("orderer", type),
                domainName, type + "s", name, "tls/server.crt").toFile();
        if (!cert.exists()) {
            throw new RuntimeException(String.format("Missing cert file for: %s. Could not find at location: %s", name,
                    cert.getAbsolutePath()));
        }

        Properties ret = new Properties();
        ret.setProperty("pemFile", cert.getAbsolutePath());

        ret.setProperty("hostnameOverride", name);
        ret.setProperty("sslProvider", "openSSL");
        ret.setProperty("negotiationType", "TLS");

        // ret.setProperty("grpc.NettyChannelBuilderOption.keepAliveTime","5");
        return ret;
    }

    public Properties getEventHubProperties(String name) {

        return getEndPointProperties("peer", name); // uses same as named peer

    }

    public String getChannelPath() {
        return path + "/artifacts/channel";
    }

    public String getConfigPath() {
        return path;
    }

    private String getDomainName(final String name) {
        int dot = name.indexOf(".");
        if (-1 == dot) {
            return null;
        } else {
            return name.substring(dot + 1);
        }

    }

    public File findFileSk(File directory) {

        File[] matches = directory.listFiles((dir, name) -> name.endsWith("_sk"));

        if (null == matches) {
            throw new RuntimeException(format("Matches returned null does %s directory exist?", directory
                    .getAbsoluteFile().getName()));
        }

        if (matches.length != 1) {
            throw new RuntimeException(format("Expected in %s only 1 sk file but found %d", directory.getAbsoluteFile()
                    .getName(), matches.length));
        }

        return matches[0];

    }

}
