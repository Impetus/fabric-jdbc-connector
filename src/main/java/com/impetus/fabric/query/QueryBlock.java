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
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.hyperledger.fabric.sdk.BlockEvent;
import org.hyperledger.fabric.sdk.ChaincodeID;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.EventHub;
import org.hyperledger.fabric.sdk.HFClient;
import org.hyperledger.fabric.sdk.InstallProposalRequest;
import org.hyperledger.fabric.sdk.InstantiateProposalRequest;
import org.hyperledger.fabric.sdk.Orderer;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.ProposalResponse;
import org.hyperledger.fabric.sdk.SDKUtils;
import org.hyperledger.fabric.sdk.TransactionProposalRequest;
import org.hyperledger.fabric.sdk.exception.CryptoException;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.TransactionEventException;
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

    private String channelName = "mychannel";

    // For setting CryptoSuite only if the application is running for the first
    // time.
    private int counter = 0;

    private Collection<Org> SampleOrgs;

    private HFClient client = HFClient.createNewInstance();

    public QueryBlock(String configPath) {
        conf = Config.getConfig(configPath);
    }
    
    public Config getConf() {
        return conf;
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
                        conf.findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
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
                        conf.findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
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
            logger.error("QueryBlock | enrollAndRegister | " + e.getMessage());
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
            logger.error("QueryBlock | reconstructChannel " + e.getMessage());
            return null;
        }

    }

    public String installChaincode(String chaincodeName, String version, String goPath, String chainCodePath) {
        Collection<ProposalResponse> responses;
        Collection<ProposalResponse> successful = new ArrayList<>();
        Collection<ProposalResponse> failed = new ArrayList<>();
        try {
            checkConfig();
            Org sampleOrg = conf.getSampleOrg("peerOrg1");
            InstallProposalRequest installProposalRequest = getInstallProposalRequest(chaincodeName, version, goPath,
                    chainCodePath, sampleOrg);
            logger.info("Sending install proposal");
            int numInstallProposal = 0;

            Set<Peer> peersFromOrg = sampleOrg.getPeers();
            numInstallProposal = numInstallProposal + peersFromOrg.size();
            responses = client.sendInstallProposal(installProposalRequest, peersFromOrg);

            for (ProposalResponse response : responses) {
                if (response.getStatus() == ProposalResponse.Status.SUCCESS) {
                    logger.debug(String.format("Successful install proposal response Txid: %s from peer %s",
                            response.getTransactionID(), response.getPeer().getName()));
                    successful.add(response);
                } else {
                    failed.add(response);
                }
            }
            SDKUtils.getProposalConsistencySets(responses);
            logger.info(String.format("Received %d install proposal responses. Successful+verified: %d . Failed: %d",
                    numInstallProposal, successful.size(), failed.size()));
            if (failed.size() > 0) {
                ProposalResponse first = failed.iterator().next();
                return "Not enough endorsers for install :" + successful.size() + ".  " + first.getMessage();
            }

            return "Chaincode installed successfully";
        } catch (Exception e) {
            logger.error("QueryBlock | installChaincode | " + e.getMessage());
            e.printStackTrace();
            return "Chaincode installation failed";
        }
    }

    public String instantiateChaincode(String chaincodeName, String chainCodeVersion, String chainCodePath,
            String chaincodeFunction, String[] chaincodeArgs) {

        Collection<ProposalResponse> responses;
        Collection<ProposalResponse> successful = new ArrayList<>();
        Collection<ProposalResponse> failed = new ArrayList<>();

        try {
            checkConfig();

            Org sampleOrg = conf.getSampleOrg("peerOrg1");
            Channel channel = reconstructChannel();
            Collection<Orderer> orderers = channel.getOrderers();
            InstantiateProposalRequest instantiateProposalRequest = getInstantiateProposalRequest(chaincodeName,
                    chainCodeVersion, chainCodePath, chaincodeFunction, chaincodeArgs, sampleOrg, channel);
            Map<String, byte[]> tm = new HashMap<>();
            tm.put("HyperLedgerFabric", "InstantiateProposalRequest:JavaSDK".getBytes(UTF_8));
            tm.put("method", "InstantiateProposalRequest".getBytes(UTF_8));
            instantiateProposalRequest.setTransientMap(tm);
            logger.info(
                    "Sending instantiateProposalRequest to all peers with arguments: a and b set to 100 and %s respectively",
                    "" + (200));

            responses = channel.sendInstantiationProposal(instantiateProposalRequest, channel.getPeers());
            for (ProposalResponse response : responses) {
                if (response.isVerified() && response.getStatus() == ProposalResponse.Status.SUCCESS) {
                    successful.add(response);
                    logger.info("Succesful instantiate proposal response Txid: %s from peer %s",
                            response.getTransactionID(), response.getPeer().getName());
                } else {
                    failed.add(response);
                }
            }
            logger.info("Received %d instantiate proposal responses. Successful+verified: %d . Failed: %d",
                    responses.size(), successful.size(), failed.size());
            if (failed.size() > 0) {
                ProposalResponse first = failed.iterator().next();

                return "Chaincode instantiation failed , reason " + "Not enough endorsers for instantiate :"
                        + successful.size() + "endorser failed with " + first.getMessage() + ". Was verified:"
                        + first.isVerified();
            }

            logger.info("Sending instantiateTransaction to orderer with a and b set to 100 and %s respectively",
                    "" + (200));
            logger.info("orderers", orderers);
            channel.sendTransaction(successful, orderers)
                    .thenApply(
                            transactionEvent -> {
                                logger.info("transaction event is valid", transactionEvent.isValid());
                                logger.info("Finished instantiate transaction with transaction id %s",
                                        transactionEvent.getTransactionID());
                                return null;
                            })
                    .exceptionally(
                            e -> {
                                if (e instanceof TransactionEventException) {
                                    BlockEvent.TransactionEvent te = ((TransactionEventException) e)
                                            .getTransactionEvent();
                                    if (te != null) {
                                        logger.info("Transaction with txid %s failed. %s", te.getTransactionID(),
                                                e.getMessage());
                                    }
                                }
                                logger.info(" failed with %s exception %s", e.getClass().getName(), e.getMessage());
                                return null;
                            }).get(conf.getTransactionWaitTime(), TimeUnit.SECONDS);

            return "Chaincode instantiated Successfully";

        } catch (Exception e) {

            logger.error("QueryBlock | instantiateChaincode |" + e.getMessage());
            return "Chaincode instantiation failed , reason " + e.getMessage();

        }

    }

    public String invokeChaincode(String chaincodename, String chaincodeFunction, String[] chaincodeArgs) {

        try {
            Collection<ProposalResponse> responses;
            Collection<ProposalResponse> successful = new ArrayList<>();
            Collection<ProposalResponse> failed = new ArrayList<>();

            checkConfig();

            ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodename).build();
            Channel channel = reconstructChannel();
            logger.info(String.format("[Channel Name:- %s, Chaincode Function:- %s, Chaincode Args:- %s]",
                    channel.getName(), chaincodeFunction, Arrays.asList(chaincodeArgs)));
            TransactionProposalRequest transactionProposalRequest = client.newTransactionProposalRequest();
            transactionProposalRequest.setChaincodeID(chaincodeID);
            transactionProposalRequest.setFcn(chaincodeFunction);
            transactionProposalRequest.setProposalWaitTime(conf.getProposalWaitTime());
            transactionProposalRequest.setArgs(chaincodeArgs);

            Map<String, byte[]> tm = new HashMap<>();
            tm.put("HyperLedgerFabric", "TransactionProposalRequest:JavaSDK".getBytes(UTF_8));
            tm.put("method", "TransactionProposalRequest".getBytes(UTF_8));
            tm.put("result", ":)".getBytes(UTF_8));
            transactionProposalRequest.setTransientMap(tm);

            logger.info("sending transactionProposal to all peers with arguments");

            responses = channel.sendTransactionProposal(transactionProposalRequest, channel.getPeers());
            for (ProposalResponse response : responses) {
                if (response.getStatus() == ProposalResponse.Status.SUCCESS) {
                    logger.info("Successful transaction proposal response Txid: " + response.getTransactionID()
                            + "from peer " + response.getPeer().getName());
                    successful.add(response);
                } else {
                    failed.add(response);
                }
            }
            Collection<Set<ProposalResponse>> proposalConsistencySets = SDKUtils.getProposalConsistencySets(responses);
            if (proposalConsistencySets.size() != 1) {
                logger.info(format("Expected only one set of consistent proposal responses but got "
                        + proposalConsistencySets.size()));
            }

            logger.info("Received " + responses.size() + " transaction proposal responses. Successful+verified: "
                    + successful.size() + " . Failed: " + failed.size());
            if (failed.size() > 0) {
                ProposalResponse firstTransactionProposalResponse = failed.iterator().next();
                logger.info("Not enough endorsers for invoke:" + failed.size() + " endorser error: "
                        + firstTransactionProposalResponse.getMessage() + ". Was verified: "
                        + firstTransactionProposalResponse.isVerified());
            }
            logger.info("Successfully received transaction proposal responses.");
            ProposalResponse resp = responses.iterator().next();
            logger.debug("getChaincodeActionResponseReadWriteSetInfo:::"
                    + resp.getChaincodeActionResponseReadWriteSetInfo());
            logger.info("Sending chaincode transaction to orderer.");
            channel.sendTransaction(successful)
                    .thenApply(transactionEvent -> {

                        logger.info("transaction event is valid " + transactionEvent.isValid());
                        logger.info("Finished invoke transaction with transaction id "
                                    + transactionEvent.getTransactionID());
                        return "Chaincode invoked successfully " + transactionEvent.getTransactionID();
                    })
                    .exceptionally(e -> {
                        if (e instanceof TransactionEventException) {
                            BlockEvent.TransactionEvent te = ((TransactionEventException) e).getTransactionEvent();
                            if (te != null) {
                                logger.info("Transaction with txid " + te.getTransactionID() + " failed. " + e.getMessage());
                        }
                    }
                    logger.info("failed with " + e.getClass().getName() + " exception " + e.getMessage());
                    return "Error";
                }   ).get(conf.getTransactionWaitTime(), TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.info("Caught an exception while invoking chaincode");
            logger.error("QueryBlock | invokeChaincode | " + e.getMessage());
            e.printStackTrace();
            return "Caught an exception while invoking chaincode";

        }
        return "Transaction invoked successfully ";
    }

    private InstallProposalRequest getInstallProposalRequest(String chaincodeName, String version, String goPath,
            String chainCodePath, Org sampleOrg) throws InvalidArgumentException {
        ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodeName).setVersion(version)
                .setPath(chainCodePath).build();
        Channel channel = reconstructChannel();
        final String channelName = channel.getName();
        logger.info(String.format("Running channel %s", channelName));

        client.setUserContext(sampleOrg.getPeerAdmin());
        logger.info("Creating install proposal");
        InstallProposalRequest installProposalRequest = client.newInstallProposalRequest();
        installProposalRequest.setChaincodeID(chaincodeID);
        installProposalRequest.setChaincodeSourceLocation(new File(goPath));
        installProposalRequest.setChaincodeVersion(version);
        return installProposalRequest;
    }

    private InstantiateProposalRequest getInstantiateProposalRequest(String chaincodeName, String chainCodeVersion,
            String chainCodePath, String chaincodeFunction, String[] chaincodeArgs, Org sampleOrg, Channel channel) {
        ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodeName).setVersion(chainCodeVersion)
                .setPath(chainCodePath).build();
        InstantiateProposalRequest instantiateProposalRequest = client.newInstantiationProposalRequest();
        instantiateProposalRequest.setProposalWaitTime(conf.getProposalWaitTime());
        instantiateProposalRequest.setChaincodeID(chaincodeID);
        instantiateProposalRequest.setFcn(chaincodeFunction);
        instantiateProposalRequest.setArgs(chaincodeArgs);
        return instantiateProposalRequest;
    }

}