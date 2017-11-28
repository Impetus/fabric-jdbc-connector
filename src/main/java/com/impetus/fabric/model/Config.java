/*
 *  
 * COPYRIGHT FOR IMPETUS
 *
 *  
 */
package com.impetus.fabric.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.hibernate.engine.transaction.jta.platform.internal.SynchronizationRegistryBasedSynchronizationStrategy;
import org.hyperledger.fabric.sdk.helper.Utils;

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

    public static final String LOGGERLEVEL = "org.hyperledger.fabric.sdk.loglevel";

    private static final String ORGS = PROPBASE + "property.";

    private static final Pattern orgPat = Pattern.compile("^" + Pattern.quote(ORGS) + "([^\\.]+)\\.mspid$");

    private static final String BLOCKCHAINTLS = PROPBASE + "blockchain.tls";

    private static Config config;

    public static final Properties sdkProperties = new Properties();

    private final boolean runningTLS;

    private final boolean runningFabricCATLS;

    private final boolean runningFabricTLS;

    private static final Map<String, Org> orgs = new HashMap<>();

    private Config(String configPath) {
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

            defaultProperty(ORGS + "peerOrg1.mspid", "Org1MSP");
            defaultProperty(ORGS + "peerOrg1.domname", "org1.example.com");
            defaultProperty(ORGS + "peerOrg1.ca_location", "http://localhost:7054");
            defaultProperty(ORGS + "peerOrg1.peer_locations",
                    "peer0.org1.example.com@grpc://localhost:7051, peer1.org1.example.com@grpc://localhost:7056");
            defaultProperty(ORGS + "peerOrg1.orderer_locations", "orderer.example.com@grpc://localhost:7050");
            defaultProperty(ORGS + "peerOrg1.eventhub_locations",
                    "peer0.org1.example.com@grpc://localhost:7053,peer1.org1.example.com@grpc://localhost:7058");

            defaultProperty(BLOCKCHAINTLS, null);
            defaultProperty(LOGGERLEVEL, "DEBUG");
            runningTLS = null != sdkProperties.getProperty(BLOCKCHAINTLS, null);
            runningFabricCATLS = runningTLS;
            runningFabricTLS = runningTLS;

            for (Map.Entry<Object, Object> x : sdkProperties.entrySet()) {
                final String key = x.getKey() + "";
                final String val = x.getValue() + "";

                if (key.startsWith(ORGS)) {

                    Matcher match = orgPat.matcher(key);

                    if (match.matches() && match.groupCount() == 1) {
                        String orgName = match.group(1).trim();
                        orgs.put(orgName, new Org(orgName, val.trim()));

                    }
                }
            }

            for (Map.Entry<String, Org> org : orgs.entrySet()) {
                final Org sampleOrg = org.getValue();
                final String orgName = org.getKey();

                String peerNames = sdkProperties.getProperty(ORGS + orgName + ".peer_locations");
                String[] ps = peerNames.split("[ \t]*,[ \t]*");
                for (String peer : ps) {
                    String[] nl = peer.split("[ \t]*@[ \t]*");
                    sampleOrg.addPeerLocation(nl[0], grpcTLSify(nl[1]));
                }

                final String domainName = sdkProperties.getProperty(ORGS + orgName + ".domname");

                sampleOrg.setDomainName(domainName);

                String ordererNames = sdkProperties.getProperty(ORGS + orgName + ".orderer_locations");
                ps = ordererNames.split("[ \t]*,[ \t]*");
                for (String peer : ps) {
                    String[] nl = peer.split("[ \t]*@[ \t]*");
                    sampleOrg.addOrdererLocation(nl[0], grpcTLSify(nl[1]));
                }

                String eventHubNames = sdkProperties.getProperty(ORGS + orgName + ".eventhub_locations");
                ps = eventHubNames.split("[ \t]*,[ \t]*");
                for (String peer : ps) {
                    String[] nl = peer.split("[ \t]*@[ \t]*");
                    sampleOrg.addEventHubLocation(nl[0], grpcTLSify(nl[1]));
                }

                sampleOrg.setCALocation(httpTLSify(sdkProperties.getProperty((ORGS + org.getKey() + ".ca_location"))));

                String cert = path
                        + "artifacts/channel/crypto-config/peerOrganizations/DNAME/ca/ca.DNAME-cert.pem".replaceAll(
                                "DNAME", domainName);
                File cf = new File(cert);
                if (!cf.exists() || !cf.isFile()) {
                    throw new RuntimeException(" missing cert file " + cf.getAbsolutePath());
                }
                Properties properties = new Properties();
                properties.setProperty("pemFile", cf.getAbsolutePath());

                properties.setProperty("allowAllHostNames", "true"); // TODO:
                                                                     // Need to
                                                                     // remove
                                                                     // for
                                                                     // production
                                                                     // ready

                sampleOrg.setCAProperties(properties);

            }

        }

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

    /**
     * getConfig return back singleton for SDK configuration.
     *
     * @return Global configuration
     */
    public static Config getConfig(String path) {
        if (null == config) {
            config = new Config(path);
        }
        return config;

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

    private static void defaultProperty(String key, String value) {

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

    public Collection<Org> getSampleOrgs() {
        return Collections.unmodifiableCollection(orgs.values());
    }

    public Org getSampleOrg(String name) {
        return orgs.get(name);

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

        /**
         * for loading properties from hyperledger.properties file
         */
        Properties hyperproperties = new Properties();
        try {
            hyperproperties.load(new FileInputStream(path + "/hyperledger.properties"));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
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

}
