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
package com.impetus.fabric.query;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.hyperledger.fabric.sdk.BlockEvent;
import org.hyperledger.fabric.sdk.ChaincodeEndorsementPolicy;
import org.hyperledger.fabric.sdk.ChaincodeID;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.EventHub;
import org.hyperledger.fabric.sdk.HFClient;
import org.hyperledger.fabric.sdk.InstallProposalRequest;
import org.hyperledger.fabric.sdk.InstantiateProposalRequest;
import org.hyperledger.fabric.sdk.Orderer;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.ProposalResponse;
import org.hyperledger.fabric.sdk.QueryByChaincodeRequest;
import org.hyperledger.fabric.sdk.SDKUtils;
import org.hyperledger.fabric.sdk.TransactionProposalRequest;
import org.hyperledger.fabric.sdk.UpgradeProposalRequest;
import org.hyperledger.fabric.sdk.exception.ChaincodeEndorsementPolicyParseException;
import org.hyperledger.fabric.sdk.exception.CryptoException;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.TransactionEventException;
import org.hyperledger.fabric.sdk.security.CryptoSuite;
import org.hyperledger.fabric_ca.sdk.HFCAClient;
import org.hyperledger.fabric_ca.sdk.RegistrationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.function.Endorsers;
import com.impetus.blkch.sql.function.PolicyFile;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.util.Tuple2;
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

@NotThreadSafe
public class QueryBlock {

    private static final Logger logger = LoggerFactory.getLogger(QueryBlock.class);

    private Config conf;

    private String adminName;

    private String channelName;

    private String adminCA;

    private String admCApw;
    
    private Channel channel;
    
    // For setting CryptoSuite only if the application is running for the first
    // time.
    private int counter = 0;

    private Collection<Org> SampleOrgs;

    private HFClient client = HFClient.createNewInstance();
    
    public QueryBlock(String configPath, String channel) {
        conf = new Config(configPath);
        channelName = channel;
        adminName = conf.getAdmin();
        adminCA = conf.getAdminCA();
        admCApw = conf.getAdminCApw();
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

    //Setting channel object for QueryBlock Object.
    public void setChannel() {
        this.channel = reconstructChannel();          
    }
    
    public Channel getChannel() {
        return channel;          
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
                logger.error("QueryBlock | checkConfig | " + e);
            }
        }

        for (Org sampleOrg : SampleOrgs) {
            try {
                sampleOrg.setCAClient(HFCAClient.createNewInstance(sampleOrg.getCALocation(),
                        sampleOrg.getCAProperties()));
            } catch (MalformedURLException e) {
                logger.error("QueryBlock | checkConfig | " + e);
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

            HyperUser user = sampleStore.getMember(name, orgName);
            sampleOrg.addUser(user);

            sampleOrg.setPeerAdmin(sampleStore.getMember(orgName + adminName, orgName));

            final String sampleOrgName = sampleOrg.getName();
            final String sampleOrgDomainName = sampleOrg.getDomainName();

            HyperUser peerOrgAdmin;

            try {
                peerOrgAdmin = sampleStore.getMember(
                        sampleOrgName + adminName,
                        sampleOrgName,
                        sampleOrg.getMSPID(),
                        conf.findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
                                sampleOrgDomainName,
                                format("/users/%s@%s/msp/keystore", adminName, sampleOrgDomainName)).toFile()),
                        Paths.get(
                                conf.getChannelPath(),
                                "crypto-config/peerOrganizations/",
                                sampleOrgDomainName,
                                format("/users/%s@%s/msp/signcerts/%s@%s-cert.pem", adminName, sampleOrgDomainName,
                                        adminName, sampleOrgDomainName)).toFile());
                sampleOrg.setPeerAdmin(peerOrgAdmin);
            } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException | IOException e) {
                String errMsg = "QueryBlock | loadUserFromPersistence | " + e;
                logger.error(errMsg);
                throw new BlkchnException(errMsg);
            }

        }
        return "Successfully loaded member from persistence";

    }

    /**
     * enroll and register user at starting takes username as input returns
     * status as string
     */
    public synchronized String enrollAndRegister(String uname) {

        try {
            checkConfig();

            File sampleStoreFile = new File(conf.getConfigPath() + "/HyperledgerEnroll.properties");

            final Store sampleStore = new Store(sampleStoreFile);
            for (Org sampleOrg : SampleOrgs) {

                HFCAClient ca = sampleOrg.getCAClient();
                final String orgName = sampleOrg.getName();
                final String mspid = sampleOrg.getMSPID();
                ca.setCryptoSuite(CryptoSuite.Factory.getCryptoSuite());
                HyperUser admin = sampleStore.getMember(orgName + "FABRIC_CA"+ adminCA, orgName);
                if (!admin.isEnrolled()) {
                    admin.setEnrollment(ca.enroll(adminCA, admCApw));
                    admin.setMspId(mspid);
                }

                if (sampleStore.hasMember(uname, sampleOrg.getName())) {
                    String result = loadUserFromPersistence(uname);
                    return result;
                }
                
                HyperUser user = sampleStore.getMember(uname, sampleOrg.getName());
                String usrAffilation = sampleOrg.getUsrAffilation();
                if (!user.isRegistered()) {
                    RegistrationRequest rr = new RegistrationRequest(user.getName(), usrAffilation);
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
                        sampleOrgName + adminName,
                        sampleOrgName,
                        sampleOrg.getMSPID(),
                        conf.findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
                                sampleOrgDomainName,
                                format("/users/%s@%s/msp/keystore", adminName, sampleOrgDomainName)).toFile()),
                        Paths.get(
                                conf.getChannelPath(),
                                "crypto-config/peerOrganizations/",
                                sampleOrgDomainName,
                                format("/users/%s@%s/msp/signcerts/%s@%s-cert.pem", adminName, sampleOrgDomainName,
                                        adminName, sampleOrgDomainName)).toFile());

                sampleOrg.setPeerAdmin(peerOrgAdmin);

            }

        } catch (Exception e) {
            String errMsg = "QueryBlock | enrollAndRegister: Failed to enroll user";
            logger.error(errMsg, e);
            throw new BlkchnException(errMsg, e);
        }
        return "User  Enrolled Successfuly";
    }
    
    public Channel reconstructChannel()
    {
        checkConfig();
        try
        {
            org.apache.log4j.Level setTo = null;
            setTo = org.apache.log4j.Level.DEBUG;
            org.apache.log4j.Logger.getLogger("org.hyperledger.fabric").setLevel(setTo);

            Org sampleOrg1 = conf.getSampleOrgs().toArray(new Org[] {})[0];

            Channel newChannel = null;
            boolean test = true;
            for (Org sampleOrg : SampleOrgs)
            {
                
                client.setUserContext(sampleOrg.getPeerAdmin());
                if (test)
                {
                    newChannel = client.newChannel(channelName);
                    for (String orderName : sampleOrg1.getOrdererNames())
                    {
                        newChannel.addOrderer(client.newOrderer(orderName, sampleOrg1.getOrdererLocation(orderName),
                                conf.getOrdererProperties(orderName)));
                    }
                    test = false;
                }

                for (String peerName : sampleOrg.getPeerNames())
                {
                    logger.debug(peerName);
                    String peerLocation = sampleOrg.getPeerLocation(peerName);
                    Peer peer = client.newPeer(peerName, peerLocation, conf.getPeerProperties(peerName));

                    Set<String> channels = client.queryChannels(peer);
                    if (!channels.contains(channelName))
                    {
                        logger.info(String.format("Peer %s does not appear to belong to channel %s", peerName, channelName));
                    }
                    newChannel.addPeer(peer);
                    sampleOrg.addPeer(peer);
                }

                for (String eventHubName : sampleOrg.getEventHubNames())
                {
                    EventHub eventHub = client.newEventHub(eventHubName, sampleOrg.getEventHubLocation(eventHubName),
                            conf.getEventHubProperties(eventHubName));
                    newChannel.addEventHub(eventHub);
                }
                newChannel.initialize();
            }
            return newChannel;

        }
        catch (Exception e)
        {
            String errMsg = "QueryBlock | reconstructChannel " + e;
            logger.error(errMsg);
            throw new BlkchnException(errMsg, e);
        }

    }
    
    public String installChaincode(String chaincodeName, String version, String goPath, String chaincodePath) {
        Collection<ProposalResponse> responses;
        Collection<ProposalResponse> successful = new ArrayList<>();
        Collection<ProposalResponse> failed = new ArrayList<>();
        int numInstallProposal = 0;
        try {
            checkConfig();
            for(Org sampleOrg : conf.getSampleOrgs()) {
                InstallProposalRequest installProposalRequest = getInstallProposalRequest(chaincodeName, version, goPath, chaincodePath, sampleOrg);
                Set<Peer> peersFromOrg = sampleOrg.getPeers();
                numInstallProposal = numInstallProposal + peersFromOrg.size();
                responses = client.sendInstallProposal(installProposalRequest, peersFromOrg);
                for(ProposalResponse response : responses) {
                    if(response.getStatus() == ProposalResponse.Status.SUCCESS) {
                        logger.debug(String.format("Successful install proposal response Txid: %s from peer %s",
                                response.getTransactionID(), response.getPeer().getName()));
                        successful.add(response);
                    } else {
                        failed.add(response);
                    }
                }
                SDKUtils.getProposalConsistencySets(responses);
                if (failed.size() > 0) {
                    ProposalResponse first = failed.iterator().next();
                    String errMsg = "Not enough endorsers for install :" + successful.size() + ".  " + first.getMessage();
                    throw new BlkchnException(errMsg);
                }
            }
            logger.info(String.format("Received %d install proposal responses. Successful+verified: %d . Failed: %d",
                    numInstallProposal, successful.size(), failed.size()));
            return "Chaincode installed successfully";
        } catch(Exception e) {
            String errMsg = "QueryBlock | installChaincode | " + e;
            logger.error(errMsg);
            throw new BlkchnException("Chaincode installation failed" + " " + errMsg);
        }
    }

    public String instantiateChaincode(String chaincodeName, String chainCodeVersion, String chainCodePath,
            String chaincodeFunction, String[] chaincodeArgs, Endorsers endorsers) {

        Collection<ProposalResponse> responses;
        Collection<ProposalResponse> successful = new ArrayList<>();
        Collection<ProposalResponse> failed = new ArrayList<>();

        try {
            checkConfig();

            Collection<Orderer> orderers = channel.getOrderers();
            InstantiateProposalRequest instantiateProposalRequest = getInstantiateProposalRequest(chaincodeName,
                    chainCodeVersion, chainCodePath, chaincodeFunction, chaincodeArgs, endorsers);
            Map<String, byte[]> tm = new HashMap<>();
            tm.put("HyperLedgerFabric", "InstantiateProposalRequest:JavaSDK".getBytes(UTF_8));
            tm.put("method", "InstantiateProposalRequest".getBytes(UTF_8));
            instantiateProposalRequest.setTransientMap(tm);
            logger.info(
                    "Sending instantiateProposalRequest to all peers with arguments:" + Arrays.asList(chaincodeArgs));

            responses = channel.sendInstantiationProposal(instantiateProposalRequest, channel.getPeers());
            for (ProposalResponse response : responses) {
                if (response.isVerified() && response.getStatus() == ProposalResponse.Status.SUCCESS) {
                    successful.add(response);
                    logger.info(String.format("Succesful instantiate proposal response Txid: %s from peer %s",
                            response.getTransactionID(), response.getPeer().getName()));
                } else {
                    failed.add(response);
                }
            }
            logger.info(String.format("Received %d instantiate proposal responses. Successful+verified: %d . Failed: %d",
                    responses.size(), successful.size(), failed.size()));
            if (failed.size() > 0) {
                ProposalResponse first = failed.iterator().next();

                String errMsg = "Chaincode instantiation failed , reason " + "Not enough endorsers for instantiate :"
                        + successful.size() + "endorser failed with " + first.getMessage() + ". Was verified:"
                        + first.isVerified();
                throw new BlkchnException(errMsg);
            }

            logger.info("Sending instantiateTransaction to orderer");
            logger.info("orderers", orderers);
            channel.sendTransaction(successful, orderers)
                    .thenApply(
                            transactionEvent -> {
                                logger.info("transaction event is valid", transactionEvent.isValid());
                                logger.info(String.format("Finished instantiate transaction with transaction id %s",
                                        transactionEvent.getTransactionID()));
                                return null;
                            }).exceptionally(e -> {
                        if (e instanceof TransactionEventException) {
                            BlockEvent.TransactionEvent te = ((TransactionEventException) e).getTransactionEvent();
                            if (te != null) {
                                logger.info(String.format("Transaction with txid %s failed. %s", te.getTransactionID(), e));
                            }
                        }
                        String errMsg = String.format(" failed with %s exception %s", e.getClass().getName(), e);
                        logger.info(errMsg);
                        // return null;
                            throw new BlkchnException(errMsg);
                        }).get(conf.getTransactionWaitTime(), TimeUnit.MILLISECONDS);

            return "Chaincode instantiated Successfully";

        } catch (Exception e) {

            String errMsg = "QueryBlock | instantiateChaincode |" + e;
            logger.error(errMsg);
            throw new BlkchnException(errMsg, e);
        }

    }
    
    public String invokeChaincode(String chaincodename, String chaincodeFunction, String[] chaincodeArgs) {

        try {
            Collection<ProposalResponse> responses;
            Collection<ProposalResponse> successful = new ArrayList<>();
            Collection<ProposalResponse> failed = new ArrayList<>();

            checkConfig();

            ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodename).build();
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
                String errMsg = "Not enough endorsers for invoke:" + failed.size() + " endorser error: "
                        + firstTransactionProposalResponse.getMessage() + ". Was verified: "
                        + firstTransactionProposalResponse.isVerified();
                logger.info(errMsg);
                throw new BlkchnException(errMsg);
            }
            logger.info("Successfully received transaction proposal responses.");
            ProposalResponse resp = responses.iterator().next();
            logger.debug("getChaincodeActionResponseReadWriteSetInfo:::"
                    + resp.getChaincodeActionResponseReadWriteSetInfo());
            logger.info("Sending chaincode transaction to orderer.");
            channel.sendTransaction(successful);
        } catch (Exception e) {
            logger.info("Caught an exception while invoking chaincode");
            String errMsg = "QueryBlock | invokeChaincode | " + e;
            logger.error(errMsg);
            throw new BlkchnException("Caught an exception while invoking chaincode" + " " + errMsg);

        }
        return "Transaction invoked successfully ";
    }

    public String queryChaincode(String chaincodename, String chaincodeFunction, String[] chaincodeArgs) {
        try {
            checkConfig();

            ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodename).build();
            logger.info("Channel Name is " + channel.getName());
            logger.debug(String.format("Querying chaincode %s and function %s with arguments %s", chaincodename,
                    chaincodeFunction, Arrays.asList(chaincodeArgs).toString()));

            QueryByChaincodeRequest queryByChaincodeRequest = client.newQueryProposalRequest();
            queryByChaincodeRequest.setArgs(chaincodeArgs);
            queryByChaincodeRequest.setFcn(chaincodeFunction);
            queryByChaincodeRequest.setChaincodeID(chaincodeID);

            Map<String, byte[]> tm2 = new HashMap<>();
            tm2.put("HyperLedgerFabric", "QueryByChaincodeRequest:JavaSDK".getBytes(UTF_8));
            tm2.put("method", "QueryByChaincodeRequest".getBytes(UTF_8));
            queryByChaincodeRequest.setTransientMap(tm2);
            logger.debug("Chaincode request args:- " + queryByChaincodeRequest.getArgs().toString());
            Collection<ProposalResponse> queryProposals = channel.queryByChaincode(queryByChaincodeRequest,
                    channel.getPeers());

            for (ProposalResponse proposalResponse : queryProposals) {
                if (!proposalResponse.isVerified() || proposalResponse.getStatus() != ProposalResponse.Status.SUCCESS) {
                    String errorMsg = "Failed query proposal from peer " + proposalResponse.getPeer().getName()
                            + " status: " + proposalResponse.getStatus() + ". Messages: "
                            + proposalResponse.getMessage() + ". Was verified : " + proposalResponse.isVerified();
                    logger.debug(errorMsg);
                    throw new BlkchnException(errorMsg);
                } else {
                    return proposalResponse.getProposalResponse().getResponse().getPayload().toStringUtf8();
                }

            }

        } catch (Exception e) {
            logger.error("QueryBlock | queryChaincode | " + e.getMessage());
            throw new BlkchnException(e);
        }
        throw new BlkchnException("Caught an exception while quering chaincode");
    }
    
    public String upgradeChaincode(String chaincodeName, String chainCodeVersion, String chainCodePath,
            String chaincodeFunction, String[] chaincodeArgs, Endorsers endorsers) {
        Collection<ProposalResponse> responses;
        Collection<ProposalResponse> successful = new ArrayList<>();
        Collection<ProposalResponse> failed = new ArrayList<>();
        
        try {
            checkConfig();
            
            Collection<Orderer> orderers = channel.getOrderers();
            UpgradeProposalRequest upgradeProposalRequest = getUpgradeProposalRequest(chaincodeName, chainCodeVersion, chainCodePath, chaincodeFunction, chaincodeArgs, endorsers);
            Map<String, byte[]> tm = new HashMap<>();
            tm.put("HyperLedgerFabric", "UpgradeProposalRequest:JavaSDK".getBytes(UTF_8));
            tm.put("method", "UpgradeProposalRequest".getBytes(UTF_8));
            upgradeProposalRequest.setTransientMap(tm);
            logger.info("Sending upgradeProposalRequest to all peers");

            responses = channel.sendUpgradeProposal(upgradeProposalRequest, channel.getPeers());
            for (ProposalResponse response : responses) {
                if (response.isVerified() && response.getStatus() == ProposalResponse.Status.SUCCESS) {
                    successful.add(response);
                    logger.info(String.format("Succesful upgrade proposal response Txid: %s from peer %s",
                            response.getTransactionID(), response.getPeer().getName()));
                } else {
                    failed.add(response);
                }
            }
            
            logger.info(String.format("Received %d upgrade proposal responses. Successful+verified: %d . Failed: %d",
                    responses.size(), successful.size(), failed.size()));
            if (failed.size() > 0) {
                ProposalResponse first = failed.iterator().next();

                String errMsg = "Chaincode upgradation failed , reason " + "Not enough endorsers for upgrade :"
                        + successful.size() + "endorser failed with " + first.getMessage() + ". Was verified:"
                        + first.isVerified();
                throw new BlkchnException(errMsg);
            }
            
            logger.info("Sending upgradeTransaction to orderer");
            logger.info("orderers", orderers);
            channel.sendTransaction(successful, orderers)
                    .thenApply(
                            transactionEvent -> {
                                logger.info("transaction event is valid", transactionEvent.isValid());
                                logger.info(String.format("Finished instantiate transaction with transaction id %s",
                                        transactionEvent.getTransactionID()));
                                return null;
                            }).exceptionally(e -> {
                        if (e instanceof TransactionEventException) {
                            BlockEvent.TransactionEvent te = ((TransactionEventException) e).getTransactionEvent();
                            if (te != null) {
                                logger.info(String.format("Transaction with txid %s failed. %s", te.getTransactionID(), e));
                            }
                        }
                        String errMsg = String.format(" failed with %s exception %s", e.getClass().getName(), e);
                        logger.info(errMsg);
                        throw new BlkchnException(errMsg);
                        }).get(conf.getTransactionWaitTime(), TimeUnit.MILLISECONDS);

            return "Chaincode upgraded Successfully";
        } catch(Exception e) {
            String errMsg = "QueryBlock | upgradeChaincode |" + e;
            logger.error(errMsg);
            throw new BlkchnException("Chaincode upgradation failed , reason " + errMsg, e);
        }
    }

    private InstallProposalRequest getInstallProposalRequest(String chaincodeName, String version, String goPath,
            String chainCodePath, Org sampleOrg) throws InvalidArgumentException {
        ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodeName).setVersion(version)
                .setPath(chainCodePath).build();
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
            String chainCodePath, String chaincodeFunction, String[] chaincodeArgs, Endorsers endorsers) {
        ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodeName).setVersion(chainCodeVersion)
                .setPath(chainCodePath).build();
        InstantiateProposalRequest instantiateProposalRequest = client.newInstantiationProposalRequest();
        instantiateProposalRequest.setProposalWaitTime(conf.getProposalWaitTime());
        instantiateProposalRequest.setChaincodeID(chaincodeID);
        instantiateProposalRequest.setFcn(chaincodeFunction);
        instantiateProposalRequest.setArgs(chaincodeArgs);
        if(endorsers != null) {
            ChaincodeEndorsementPolicy policy = getChaincodeEndorsementPolicy(endorsers);
            instantiateProposalRequest.setChaincodeEndorsementPolicy(policy);
        }
        return instantiateProposalRequest;
    }
    
    private UpgradeProposalRequest getUpgradeProposalRequest(String chaincodeName, String chainCodeVersion,
            String chainCodePath, String chaincodeFunction, String[] chaincodeArgs, Endorsers endorsers) {
        ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(chaincodeName).setVersion(chainCodeVersion)
                .setPath(chainCodePath).build();
        UpgradeProposalRequest upgradeProposalRequest = client.newUpgradeProposalRequest();
        upgradeProposalRequest.setProposalWaitTime(conf.getProposalWaitTime());
        upgradeProposalRequest.setChaincodeID(chaincodeID);
        upgradeProposalRequest.setFcn(chaincodeFunction);
        upgradeProposalRequest.setArgs(chaincodeArgs);
        if(endorsers != null) {
            ChaincodeEndorsementPolicy policy = getChaincodeEndorsementPolicy(endorsers);
            upgradeProposalRequest.setChaincodeEndorsementPolicy(policy);
        }
        return upgradeProposalRequest;
    }
    
    private ChaincodeEndorsementPolicy getChaincodeEndorsementPolicy(Endorsers endorsers) {
        ChaincodeEndorsementPolicy policy = new ChaincodeEndorsementPolicy();
        PolicyFile policyFile = endorsers.getChildType(PolicyFile.class, 0);
        String fileLoc = policyFile.getFileLocation().replace("'", "").replace("\"", "");
        try {
            policy.fromYamlFile(new File(fileLoc));
        } catch (ChaincodeEndorsementPolicyParseException | IOException e) {
            String errMsg = "QueryBlock | getChaincodeEndorsementPolicy |" + e;
            logger.error(errMsg);
            throw new BlkchnException(errMsg, e);
        }
        return policy;
    }

}
