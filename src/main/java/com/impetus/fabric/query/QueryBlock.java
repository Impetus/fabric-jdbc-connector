/*******************************************************************************
 * * Copyright 2017 Impetus Infotech.
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
package com.impetus.fabric.query;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.hyperledger.fabric.sdk.ChaincodeID;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.EventHub;
import org.hyperledger.fabric.sdk.HFClient;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.exception.CryptoException;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.security.CryptoSuite;
import org.hyperledger.fabric_ca.sdk.HFCAClient;
import org.hyperledger.fabric_ca.sdk.RegistrationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.fabric.model.Config;
import com.impetus.fabric.model.HyperUser;
import com.impetus.fabric.model.Org;
import com.impetus.fabric.model.Store;

/**
 *
 * class for implementing all the services of blockchain such as create channel,
 * recreate channel, install chaincode, instantiate chaincode, invoke chaincode,
 * query chaincode
 *
 */

public class QueryBlock {

    private static final Logger logger = LoggerFactory.getLogger(QueryBlock.class);

    private Config conf;

    private String adminName = "admin";

    private String chainCodePath;

    private String chainCodeVersion;

    private String channelName = "mychannel";

    // For setting CryptoSuite only if the application is running for the first
    // time.
    int counter = 0;

    ChaincodeID chaincodeID;

    private Collection<Org> SampleOrgs;

    HFClient client = HFClient.createNewInstance();

    public QueryBlock(String configPath) {
        conf = Config.getConfig(configPath);
    }

    public String getAdminName() {
        return adminName;
    }

    public void setAdminName(String adminName) {
        this.adminName = adminName;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    /**
     * takes input as chaincode name and returns chaincode id
     *
     * @param name
     * @return ChaincodeID
     */
    public ChaincodeID getChaincodeId(String name) {
        chaincodeID = ChaincodeID.newBuilder().setName(name).setVersion(chainCodeVersion).setPath(chainCodePath)
                .build();
        return chaincodeID;
    }

    /**
     * checking config at starting
     */
    public void checkConfig() {

        SampleOrgs = conf.getSampleOrgs();
        if (counter == 0) {
            try {
                client.setCryptoSuite(CryptoSuite.Factory.getCryptoSuite());
                counter++;
            } catch (CryptoException | InvalidArgumentException e) {
                logger.error("QueryBlock | checkConfig | " + e.getMessage());
            }
        }

        for (Org sampleOrg : SampleOrgs) {
            try {
                sampleOrg.setCAClient(HFCAClient.createNewInstance(sampleOrg.getCALocation(),
                        sampleOrg.getCAProperties()));
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        org.apache.log4j.Level setTo = null;
        setTo = org.apache.log4j.Level.DEBUG;
        org.apache.log4j.Logger.getLogger("org.hyperledger.fabric").setLevel(setTo);
    }

    /**
     * For loading user from persistence,if user already exists by taking as
     * input username
     *
     * @param name
     * @return status as string
     */

    public String loadUserFromPersistence(String name) {

        File sampleStoreFile = new File(conf.getConfigPath() + "/HyperledgerEnroll.properties");

        final Store sampleStore = new Store(sampleStoreFile);
        for (Org sampleOrg : SampleOrgs) {

            final String orgName = sampleOrg.getName();
            HyperUser admin = sampleStore.getMember(adminName, orgName);
            sampleOrg.setAdmin(admin);

            HyperUser user = sampleStore.getMember(name, orgName);
            sampleOrg.addUser(user);

            sampleOrg.setPeerAdmin(sampleStore.getMember(orgName + "Admin", orgName));

            final String sampleOrgName = sampleOrg.getName();
            final String sampleOrgDomainName = sampleOrg.getDomainName();

            HyperUser peerOrgAdmin;

            try {
                peerOrgAdmin = sampleStore.getMember(
                        sampleOrgName + "Admin",
                        sampleOrgName,
                        sampleOrg.getMSPID(),
                        findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
                                sampleOrgDomainName, format("/users/Admin@%s/msp/keystore", sampleOrgDomainName))
                                .toFile()),
                        Paths.get(
                                conf.getChannelPath(),
                                "crypto-config/peerOrganizations/",
                                sampleOrgDomainName,
                                format("/users/Admin@%s/msp/signcerts/Admin@%s-cert.pem", sampleOrgDomainName,
                                        sampleOrgDomainName)).toFile());
                sampleOrg.setPeerAdmin(peerOrgAdmin);
            } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException | IOException e) {
                logger.error("QueryBlock | loadUserFromPersistence | " + e.getMessage());
            }

        }
        return "Successfully loaded member from persistence";

    }

    /**
     * enroll and register user at starting takes username as input returns
     * status as string
     */
    public String enrollAndRegister(String uname) {

        try {
            checkConfig();

            File sampleStoreFile = new File(conf.getConfigPath() + "/HyperledgerEnroll.properties");

            final Store sampleStore = new Store(sampleStoreFile);
            for (Org sampleOrg : SampleOrgs) {

                HFCAClient ca = sampleOrg.getCAClient();
                final String orgName = sampleOrg.getName();
                final String mspid = sampleOrg.getMSPID();
                ca.setCryptoSuite(CryptoSuite.Factory.getCryptoSuite());
                HyperUser admin = sampleStore.getMember(adminName, orgName);
                if (!admin.isEnrolled()) {
                    admin.setEnrollment(ca.enroll(admin.getName(), "adminpw"));
                    admin.setMspId(mspid);
                }

                sampleOrg.setAdmin(admin);

                if (sampleStore.hasMember(uname, sampleOrg.getName())) {
                    String result = loadUserFromPersistence(uname);
                    return result;
                }
                HyperUser user = sampleStore.getMember(uname, sampleOrg.getName());

                if (!user.isRegistered()) {
                    RegistrationRequest rr = new RegistrationRequest(user.getName(), "org1.department1");

                    user.setEnrollmentSecret(ca.register(rr, admin));

                }
                if (!user.isEnrolled()) {
                    user.setEnrollment(ca.enroll(user.getName(), user.getEnrollmentSecret()));
                    user.setMspId(mspid);
                }
                sampleOrg.addUser(user);
                final String sampleOrgName = sampleOrg.getName();
                final String sampleOrgDomainName = sampleOrg.getDomainName();

                HyperUser peerOrgAdmin = sampleStore.getMember(
                        sampleOrgName + "Admin",
                        sampleOrgName,
                        sampleOrg.getMSPID(),
                        findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
                                sampleOrgDomainName, format("/users/Admin@%s/msp/keystore", sampleOrgDomainName))
                                .toFile()),
                        Paths.get(
                                conf.getChannelPath(),
                                "crypto-config/peerOrganizations/",
                                sampleOrgDomainName,
                                format("/users/Admin@%s/msp/signcerts/Admin@%s-cert.pem", sampleOrgDomainName,
                                        sampleOrgDomainName)).toFile());

                sampleOrg.setPeerAdmin(peerOrgAdmin);

                return "User  Enrolled Successfuly";

            }

        } catch (Exception e) {
            logger.error("ChaincodeServiceImpl | enrollAndRegister | " + e.getMessage());
            return "Failed to enroll user";

        }
        return "Something went wrong";
    }

    public Channel reconstructChannel() {

        checkConfig();
        try {
            org.apache.log4j.Level setTo = null;
            setTo = org.apache.log4j.Level.DEBUG;
            org.apache.log4j.Logger.getLogger("org.hyperledger.fabric").setLevel(setTo);

            Org sampleOrg = conf.getSampleOrg("peerOrg1");
            Properties per = new Properties();

            client.setUserContext(sampleOrg.getPeerAdmin());
            Channel newChannel = client.newChannel(channelName);

            for (String orderName : sampleOrg.getOrdererNames()) {

                newChannel.addOrderer(client.newOrderer(orderName, sampleOrg.getOrdererLocation(orderName),
                        conf.getOrdererProperties(orderName)));
            }
            for (String peerName : sampleOrg.getPeerNames()) {
                logger.debug(peerName);
                String peerLocation = sampleOrg.getPeerLocation(peerName);
                Peer peer = client.newPeer(peerName, peerLocation, conf.getPeerProperties(peerName));

                Set<String> channels = client.queryChannels(peer);
                if (!channels.contains(channelName)) {
                    logger.info("Peer %s does not appear to belong to channel %s", peerName, channelName);
                }
                newChannel.addPeer(peer);
                sampleOrg.addPeer(peer);
            }

            for (String eventHubName : sampleOrg.getEventHubNames()) {
                EventHub eventHub = client.newEventHub(eventHubName, sampleOrg.getEventHubLocation(eventHubName),
                        conf.getEventHubProperties(eventHubName));
                newChannel.addEventHub(eventHub);
            }
            newChannel.initialize();
            return newChannel;
        } catch (Exception e) {
            logger.error("ChaincodeServiceImpl | reconstructChannel " + e.getMessage());
            return null;
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