
package com.impetus.fabric.query;

//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.PropertySource;

//import org.springframework.stereotype.Service;

import static java.lang.String.format;
import static org.hyperledger.fabric.sdk.BlockInfo.EnvelopeType.TRANSACTION_ENVELOPE;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset;
import org.hyperledger.fabric.protos.msp.Identities;
import org.hyperledger.fabric.sdk.BlockInfo;
import org.hyperledger.fabric.sdk.BlockchainInfo;
import org.hyperledger.fabric.sdk.ChaincodeID;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.EventHub;
import org.hyperledger.fabric.sdk.HFClient;
import org.hyperledger.fabric.sdk.Peer;
import org.hyperledger.fabric.sdk.TxReadWriteSetInfo;
import org.hyperledger.fabric.sdk.BlockInfo.EndorserInfo;
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

//import static org.junit.Assert.fail;

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

	public QueryBlock(String configPath) {
		conf = Config.getConfig(configPath);
	}

	ChaincodeID chaincodeID;

	private Collection<Org> SampleOrgs;

	HFClient client = HFClient.createNewInstance();

	static void out(String format, Object... args) {

		System.err.flush();
		System.out.flush();

		System.out.println(format(format, args));
		System.err.flush();
		System.out.flush();

	}

	static String printableString(final String string) {
		int maxLogStringLength = 64;
		if (string == null || string.length() == 0) {
			return string;
		}

		String ret = string.replaceAll("[^\\p{Print}]", "?");

		ret = ret.substring(0, Math.min(ret.length(), maxLogStringLength))
				+ (ret.length() > maxLogStringLength ? "..." : "");

		return ret;

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
				// TODO Auto-generated catch block
				logger.error("QueryBlock | checkConfig | " + e.getMessage());
			}
		}

		// Set up hfca for each sample org

		for (Org sampleOrg : SampleOrgs) {
			try {
				sampleOrg.setCAClient(
						HFCAClient.createNewInstance(sampleOrg.getCALocation(), sampleOrg.getCAProperties()));
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		org.apache.log4j.Level setTo = null;
		setTo = org.apache.log4j.Level.DEBUG;
		org.apache.log4j.Logger.getLogger("org.hyperledger.fabric").setLevel(setTo);
	}

	private void waitOnFabric(int additional) {
		// NOOP today
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
			sampleOrg.setAdmin(admin); // The admin of this org.

			// No need to enroll or register all done in End2endIt !
			HyperUser user = sampleStore.getMember(name, orgName);
			sampleOrg.addUser(user); // Remember user belongs to this Org

			sampleOrg.setPeerAdmin(sampleStore.getMember(orgName + "Admin", orgName));

			final String sampleOrgName = sampleOrg.getName();
			final String sampleOrgDomainName = sampleOrg.getDomainName();

			HyperUser peerOrgAdmin;

			try {
				peerOrgAdmin = sampleStore.getMember(sampleOrgName + "Admin", sampleOrgName, sampleOrg.getMSPID(),
						findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
								sampleOrgDomainName, format("/users/Admin@%s/msp/keystore", sampleOrgDomainName))
								.toFile()),
						Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/", sampleOrgDomainName,
								format("/users/Admin@%s/msp/signcerts/Admin@%s-cert.pem", sampleOrgDomainName,
										sampleOrgDomainName))
								.toFile());
				sampleOrg.setPeerAdmin(peerOrgAdmin);
			} catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException | IOException e) {
				// TODO Auto-generated catch block
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

		// TODO Auto-generated method stub
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
				if (!admin.isEnrolled()) { // Preregistered admin only needs to
					// be enrolled with Fabric caClient.
					admin.setEnrollment(ca.enroll(admin.getName(), "adminpw"));
					admin.setMspId(mspid);
				}

				sampleOrg.setAdmin(admin); // The admin of this org --

				if (sampleStore.hasMember(uname, sampleOrg.getName())) {
					String result = loadUserFromPersistence(uname);
					return result;
				}
				HyperUser user = sampleStore.getMember(uname, sampleOrg.getName());

				if (!user.isRegistered()) { // users need to be registered AND
					// enrolled
					RegistrationRequest rr = new RegistrationRequest(user.getName(), "org1.department1");

					user.setEnrollmentSecret(ca.register(rr, admin));

				}
				if (!user.isEnrolled()) {
					user.setEnrollment(ca.enroll(user.getName(), user.getEnrollmentSecret()));
					user.setMspId(mspid);
				}
				sampleOrg.addUser(user); // Remember user belongs to this Org
				final String sampleOrgName = sampleOrg.getName();
				final String sampleOrgDomainName = sampleOrg.getDomainName();

				HyperUser peerOrgAdmin = sampleStore.getMember(sampleOrgName + "Admin", sampleOrgName,
						sampleOrg.getMSPID(),
						findFileSk(Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/",
								sampleOrgDomainName, format("/users/Admin@%s/msp/keystore", sampleOrgDomainName))
								.toFile()),
						Paths.get(conf.getChannelPath(), "crypto-config/peerOrganizations/", sampleOrgDomainName,
								format("/users/Admin@%s/msp/signcerts/Admin@%s-cert.pem", sampleOrgDomainName,
										sampleOrgDomainName))
								.toFile());

				sampleOrg.setPeerAdmin(peerOrgAdmin);

				// return "User " + user.getName() + " Enrolled Successfuly";
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
			System.out.println("1...");

			client.setUserContext(sampleOrg.getPeerAdmin());
			System.out.println("2...");
			Channel newChannel = client.newChannel(channelName);
			System.out.println("3...");

			for (String orderName : sampleOrg.getOrdererNames()) {
				System.out.println("4...");
				System.out.println("Order Name " + orderName);
				System.out.println("Order Name " + sampleOrg.getOrdererLocation(orderName));
				// orderName="172.25.41.74";

				newChannel.addOrderer(client.newOrderer(orderName, sampleOrg.getOrdererLocation(orderName),
						conf.getOrdererProperties(orderName)));
				// per.setProperty("ordererWaitTimeMilliSecs", "50000");
				// newChannel.addOrderer(client.newOrderer(orderName,"grpc://172.25.41.74:7050",per));

			}
			System.out.println("6...");
			for (String peerName : sampleOrg.getPeerNames()) {
				System.out.println("611...");
				logger.debug(peerName);
				String peerLocation = sampleOrg.getPeerLocation(peerName);
				System.out.println("6222...");

				System.out.println(peerLocation);
				System.out.println(peerName);
				Peer peer = client.newPeer(peerName, peerLocation, conf.getPeerProperties(peerName));

				// Peer peer = client.newPeer(peerName,
				// "grpc://172.25.41.74:7051", per);
				System.out.println("7...");

				// Query the actual peer for which channels it belongs to and
				// check^M
				// it belongs to this channel^M
				Set<String> channels = client.queryChannels(peer);
				if (!channels.contains(channelName)) {
					logger.info("Peer %s does not appear to belong to channel %s", peerName, channelName);
				}
				newChannel.addPeer(peer);
				sampleOrg.addPeer(peer);
			}
			System.out.println("8...");

			for (String eventHubName : sampleOrg.getEventHubNames()) {
				EventHub eventHub = client.newEventHub(eventHubName, sampleOrg.getEventHubLocation(eventHubName),
						conf.getEventHubProperties(eventHubName));
				System.out.println(eventHubName);
				System.out.println(sampleOrg.getEventHubLocation(eventHubName));
				// EventHub eventHub = client.newEventHub(eventHubName,
				// sampleOrg.getEventHubLocation(eventHubName),null);
				newChannel.addEventHub(eventHub);
			}
			System.out.println("9...");
			newChannel.initialize();
			System.out.println("10...");

			return newChannel;
		} catch (Exception e) {
			logger.error("ChaincodeServiceImpl | reconstructChannel " + e.getMessage());
			return null;
		}

	}

	/**
	 * gives blockchain info
	 */
	public String blockchainInfo() {

		String returnQuery = "";
		try {
			checkConfig();

			Channel channel = reconstructChannel();
			String channelName = channel.getName();
			// Set<Peer> peerSet = sampleOrg.getPeers();
			// Peer queryPeer = peerSet.iterator().next();
			// out("Using peer %s for channel queries", queryPeer.getName());

			BlockchainInfo channelInfo = channel.queryBlockchainInfo();
			logger.info("Channel info for : " + channelName);
			logger.info("Channel height: " + channelInfo.getHeight());

			String chainCurrentHash = Hex.encodeHexString(channelInfo.getCurrentBlockHash());
			String chainPreviousHash = Hex.encodeHexString(channelInfo.getPreviousBlockHash());
			logger.info("Chain current block hash: " + chainCurrentHash);
			logger.info("Chainl previous block hash: " + chainPreviousHash);

			// Query by block number. Should return latest block, i.e. block
			// number 2
			for (long j = channelInfo.getHeight() - 1; j > 0; --j) {
				BlockInfo returnedBlock = channel.queryBlockByNumber(j);
				String previousHash = Hex.encodeHexString(returnedBlock.getPreviousHash());
				String dataHash = Hex.encodeHexString(returnedBlock.getDataHash());
				final int transCount = returnedBlock.getEnvelopeCount();
				String transactionData = Hex.encodeHexString(returnedBlock.getTransActionsMetaData());
				logger.info("queryBlockByNumber returned correct block with blockNumber "
						+ returnedBlock.getBlockNumber() + " \n previous_hash " + previousHash);
				returnQuery += "|Block Number=" + j + " previousHash= " + previousHash + " dataHash=" + dataHash
						+ " TransCount = " + transCount + "transactionData = " + transactionData;
				for (BlockInfo.EnvelopeInfo envelopeInfo : returnedBlock.getEnvelopeInfos()) {
					returnQuery += " Transaction ID " + envelopeInfo.getTransactionID();
					returnQuery += " Channel ID " + envelopeInfo.getChannelId();
					returnQuery += " Transaction Type " + envelopeInfo.getType();
					if (envelopeInfo.getType() == TRANSACTION_ENVELOPE) {
						BlockInfo.TransactionEnvelopeInfo transactionEnvelopeInfo = (BlockInfo.TransactionEnvelopeInfo) envelopeInfo;
						returnQuery += " Transaction Action " + transactionEnvelopeInfo.getTransactionActionInfoCount();
						for (BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo transactionActionInfo : transactionEnvelopeInfo
								.getTransactionActionInfos()) {
							returnQuery += " Transaction Response Status " + transactionActionInfo.getResponseStatus();
							returnQuery += " Transaction response message bytes " + printableString(
									new String(transactionActionInfo.getResponseMessageBytes(), "UTF-8"));
							returnQuery += " Transaction Args ";
							for (int z = 0; z < transactionActionInfo.getChaincodeInputArgsCount(); ++z) {
								returnQuery += printableString(
										new String(transactionActionInfo.getChaincodeInputArgs(z), "UTF-8")) + " ";
							}
							returnQuery += " Transaction Payload " + printableString(
									new String(transactionActionInfo.getProposalResponsePayload(), "UTF-8"));
							TxReadWriteSetInfo rwsetInfo = transactionActionInfo.getTxReadWriteSet();
							if (null != rwsetInfo) {
								returnQuery += " Transaction count name space read write sets"
										+ rwsetInfo.getNsRwsetCount();
								for (TxReadWriteSetInfo.NsRwsetInfo nsRwsetInfo : rwsetInfo.getNsRwsetInfos()) {
									final String namespace = nsRwsetInfo.getNamespace();
									KvRwset.KVRWSet rws = nsRwsetInfo.getRwset();

									for (KvRwset.KVRead readList : rws.getReadsList()) {
										returnQuery += " Transaction Namespace " + namespace + " Transaction Read Key "
												+ readList.getKey() + " BLOCK NUMBER "
												+ readList.getVersion().getBlockNum() + " TRANS VERS "
												+ readList.getVersion().getTxNum();
									}
									for (KvRwset.KVWrite writeList : rws.getWritesList()) {
										String valAsString = printableString(
												new String(writeList.getValue().toByteArray(), "UTF-8"));
										returnQuery += " TRANS WRITE SET KEY " + writeList.getKey() + " VALUE "
												+ valAsString;
									}
								}
							}
						}
					}
				}
				returnQuery += " \n";
			}
			// Query by block hash. Using latest block's previous hash so should
			// return block number 1
            /*
             * byte[] hashQuery = returnedBlock.getPreviousHash(); returnedBlock
             * = channel.queryBlockByHash(hashQuery);
             * logger.info("queryBlockByHash returned block with blockNumber " +
             * returnedBlock.getBlockNumber());
             */

		} catch (Exception e) {
			logger.error("QueryBlock | blockchainInfo | " + e.getMessage());
		}
		return returnQuery;
	}

	public List<Object[]> blockchainInfoList() {
		List<Object[]> returnList = new ArrayList<>();

		try {
			checkConfig();

			Channel channel = reconstructChannel();
			String channelName = channel.getName();
			// Set<Peer> peerSet = sampleOrg.getPeers();
			// Peer queryPeer = peerSet.iterator().next();
			// out("Using peer %s for channel queries", queryPeer.getName());

			BlockchainInfo channelInfo = channel.queryBlockchainInfo();
			logger.info("Channel info for : " + channelName);
			logger.info("Channel height: " + channelInfo.getHeight());

			String chainCurrentHash = Hex.encodeHexString(channelInfo.getCurrentBlockHash());
			String chainPreviousHash = Hex.encodeHexString(channelInfo.getPreviousBlockHash());
			logger.info("Chain current block hash: " + chainCurrentHash);
			logger.info("Chainl previous block hash: " + chainPreviousHash);

			// Query by block number. Should return latest block, i.e. block
			// number 2
			for (long j = channelInfo.getHeight() - 1; j > 0; --j) {
				BlockInfo returnedBlock = channel.queryBlockByNumber(j);
				String previousHash = Hex.encodeHexString(returnedBlock.getPreviousHash());
				String dataHash = Hex.encodeHexString(returnedBlock.getDataHash());
				final int transCount = returnedBlock.getEnvelopeCount();
				String transactionData = Hex.encodeHexString(returnedBlock.getTransActionsMetaData());
				logger.info("queryBlockByNumber returned correct block with blockNumber "
						+ returnedBlock.getBlockNumber() + " \n previous_hash " + previousHash);
				for (BlockInfo.EnvelopeInfo envelopeInfo : returnedBlock.getEnvelopeInfos()) {
					List<Object> record = new ArrayList<>();
					record.add(j);
					record.add(transCount);
					record.add(transactionData);
					record.add(envelopeInfo.getTransactionID());
					record.add(envelopeInfo.getChannelId());
					record.add(envelopeInfo.getType());
					System.out.println("Record:- " + record);
                    /*
                     * if (envelopeInfo.getType() == TRANSACTION_ENVELOPE) {
                     * BlockInfo.TransactionEnvelopeInfo transactionEnvelopeInfo
                     * = (BlockInfo.TransactionEnvelopeInfo) envelopeInfo;
                     * returnQuery += " Transaction Action " +
                     * transactionEnvelopeInfo.getTransactionActionInfoCount();
                     * for
                     * (BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo
                     * transactionActionInfo : transactionEnvelopeInfo
                     * .getTransactionActionInfos()) { returnQuery +=
                     * " Transaction Response Status " +
                     * transactionActionInfo.getResponseStatus(); returnQuery +=
                     * " Transaction response message bytes " + printableString(
                     * new
                     * String(transactionActionInfo.getResponseMessageBytes(),
                     * "UTF-8")); returnQuery += " Transaction Args "; for (int
                     * z = 0; z <
                     * transactionActionInfo.getChaincodeInputArgsCount(); ++z)
                     * { returnQuery += printableString( new
                     * String(transactionActionInfo.getChaincodeInputArgs(z),
                     * "UTF-8")) + " "; } returnQuery += " Transaction Payload "
                     * + printableString( new
                     * String(transactionActionInfo.getProposalResponsePayload()
                     * , "UTF-8")); TxReadWriteSetInfo rwsetInfo =
                     * transactionActionInfo.getTxReadWriteSet(); if (null !=
                     * rwsetInfo) { returnQuery +=
                     * " Transaction count name space read write sets" +
                     * rwsetInfo.getNsRwsetCount(); for
                     * (TxReadWriteSetInfo.NsRwsetInfo nsRwsetInfo :
                     * rwsetInfo.getNsRwsetInfos()) { final String namespace =
                     * nsRwsetInfo.getNamespace(); KvRwset.KVRWSet rws =
                     * nsRwsetInfo.getRwset();
                     *
                     * for (KvRwset.KVRead readList : rws.getReadsList()) {
                     * returnQuery += " Transaction Namespace " + namespace +
                     * " Transaction Read Key " + readList.getKey() +
                     * " BLOCK NUMBER " + readList.getVersion().getBlockNum() +
                     * " TRANS VERS " + readList.getVersion().getTxNum(); } for
                     * (KvRwset.KVWrite writeList : rws.getWritesList()) {
                     * String valAsString = printableString( new
                     * String(writeList.getValue().toByteArray(), "UTF-8"));
                     * returnQuery += " TRANS WRITE SET KEY " +
                     * writeList.getKey() + " VALUE " + valAsString; } } } } }
                     */
					returnList.add(record.toArray());
				}
			}
			// Query by block hash. Using latest block's previous hash so should
			// return block number 1
            /*
             * byte[] hashQuery = returnedBlock.getPreviousHash(); returnedBlock
             * = channel.queryBlockByHash(hashQuery);
             * logger.info("queryBlockByHash returned block with blockNumber " +
             * returnedBlock.getBlockNumber());
             */

		} catch (Exception e) {
			logger.error("QueryBlock | blockchainInfo | " + e.getMessage());
			e.printStackTrace();
		}
		return returnList;
	}

	public String QueryBlockWithId(long blockNum) {

		String returnQuery = "";
		try {
			checkConfig();

			Channel channel = reconstructChannel();
			String channelName = channel.getName();
			// Set<Peer> peerSet = sampleOrg.getPeers();
			// Peer queryPeer = peerSet.iterator().next();
			// out("Using peer %s for channel queries", queryPeer.getName());

			BlockchainInfo channelInfo = channel.queryBlockchainInfo();
			logger.info("Channel info for : " + channelName);
			logger.info("Channel height: " + channelInfo.getHeight());

			String chainCurrentHash = Hex.encodeHexString(channelInfo.getCurrentBlockHash());
			String chainPreviousHash = Hex.encodeHexString(channelInfo.getPreviousBlockHash());
			logger.info("Chain current block hash: " + chainCurrentHash);
			logger.info("Chainl previous block hash: " + chainPreviousHash);

			// Query by block number. Should return latest block, i.e. block
			// number 2

			BlockInfo returnedBlock = channel.queryBlockByNumber(blockNum);
			String previousHash = Hex.encodeHexString(returnedBlock.getPreviousHash());
			String dataHash = Hex.encodeHexString(returnedBlock.getDataHash());
			final int transCount = returnedBlock.getEnvelopeCount();
			String transactionData = Hex.encodeHexString(returnedBlock.getTransActionsMetaData());
			logger.info("queryBlockByNumber returned correct block with blockNumber " + returnedBlock.getBlockNumber()
					+ " \n previous_hash " + previousHash);
			returnQuery += "|Block Number=" + blockNum + "\npreviousHash= " + previousHash + "\ndataHash=" + dataHash
					+ "\nTransCount = " + transCount + "\ntransactionData = " + transactionData;
			for (BlockInfo.EnvelopeInfo envelopeInfo : returnedBlock.getEnvelopeInfos()) {
				returnQuery += "\nTransaction ID " + envelopeInfo.getTransactionID();
				returnQuery += "\nChannel ID " + envelopeInfo.getChannelId();
				returnQuery += "\nTransaction Type " + envelopeInfo.getType();
				if (envelopeInfo.getType() == TRANSACTION_ENVELOPE) {
					BlockInfo.TransactionEnvelopeInfo transactionEnvelopeInfo = (BlockInfo.TransactionEnvelopeInfo) envelopeInfo;
					returnQuery += "\nTransaction Action " + transactionEnvelopeInfo.getTransactionActionInfoCount();
					for (BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo transactionActionInfo : transactionEnvelopeInfo
							.getTransactionActionInfos()) {
						returnQuery += "\nTransaction Response Status " + transactionActionInfo.getResponseStatus();
						returnQuery += "\nTransaction response message bytes "
								+ printableString(new String(transactionActionInfo.getResponseMessageBytes(), "UTF-8"));
						returnQuery += "\nTransaction Args ";
						for (int z = 0; z < transactionActionInfo.getChaincodeInputArgsCount(); ++z) {
							returnQuery += printableString(
									new String(transactionActionInfo.getChaincodeInputArgs(z), "UTF-8")) + " ";
						}
						returnQuery += "\nTransaction Payload " + printableString(
								new String(transactionActionInfo.getProposalResponsePayload(), "UTF-8"));
						TxReadWriteSetInfo rwsetInfo = transactionActionInfo.getTxReadWriteSet();
						if (null != rwsetInfo) {
							returnQuery += " \nTransaction count name space read write sets"
									+ rwsetInfo.getNsRwsetCount();
							for (TxReadWriteSetInfo.NsRwsetInfo nsRwsetInfo : rwsetInfo.getNsRwsetInfos()) {
								final String namespace = nsRwsetInfo.getNamespace();
								KvRwset.KVRWSet rws = nsRwsetInfo.getRwset();

								for (KvRwset.KVRead readList : rws.getReadsList()) {
									returnQuery += "\nTransaction Namespace " + namespace + "\nTransaction Read Key "
											+ readList.getKey() + "\nBLOCK NUMBER "
											+ readList.getVersion().getBlockNum() + "\nTRANS VERS "
											+ readList.getVersion().getTxNum();
								}
								for (KvRwset.KVWrite writeList : rws.getWritesList()) {
									String valAsString = printableString(
											new String(writeList.getValue().toByteArray(), "UTF-8"));
									returnQuery += "\nTRANS WRITE SET KEY " + writeList.getKey() + " VALUE "
											+ valAsString;
								}
							}
						}
					}
				}
			}
			returnQuery += " \n";

		} catch (Exception e) {
			logger.error("QueryBlock | QueryBlockWithId | " + e.getMessage());
		}
		return returnQuery;
	}

	public File findFileSk(File directory) {

		File[] matches = directory.listFiles((dir, name) -> name.endsWith("_sk"));
		System.out.println(directory.toString());

		if (null == matches) {
			throw new RuntimeException(
					format("Matches returned null does %s directory exist?", directory.getAbsoluteFile().getName()));
		}

		if (matches.length != 1) {
			throw new RuntimeException(format("Expected in %s only 1 sk file but found %d",
					directory.getAbsoluteFile().getName(), matches.length));
		}

		return matches[0];

	}

	public List<Object[]> GetBlock(long blockNum) {
		List<Object[]> returnList = new ArrayList<>();

		try {
			checkConfig();

			Channel channel = reconstructChannel();
			String channelName = channel.getName();

			if (blockNum != -1) {
				BlockInfo returnedBlock = channel.queryBlockByNumber(blockNum);

				FabricBlock fabObj = new FabricBlock();
				fabObj.setBlockHash(Hex.encodeHexString(returnedBlock.getDataHash()));
				fabObj.setPrevBlockHash(Hex.encodeHexString(returnedBlock.getPreviousHash()));
				fabObj.setBlockId(blockNum);
				fabObj.setChannelName(channelName);
				fabObj.setTransacCount(returnedBlock.getEnvelopeCount());
				returnList.add(fabObj.getRecordData().toArray());
			} else {
				BlockchainInfo channelInfo = channel.queryBlockchainInfo();
				for (long j = channelInfo.getHeight() - 1; j > 0; --j) {
					BlockInfo returnedBlock = channel.queryBlockByNumber(j);

					FabricBlock fabObj = new FabricBlock();
					fabObj.setBlockHash(Hex.encodeHexString(returnedBlock.getDataHash()));
					fabObj.setPrevBlockHash(Hex.encodeHexString(returnedBlock.getPreviousHash()));
					fabObj.setBlockId(j);
					fabObj.setChannelName(channelName);
					fabObj.setTransacCount(returnedBlock.getEnvelopeCount());
					returnList.add(fabObj.getRecordData().toArray());
				}
			}
		} catch (Exception e) {
			logger.error("QueryBlock | QueryBlockWithId | " + e.getMessage());
		}
		return returnList;
	}

	public FabricTransaction QueryTransactionWithBlkID(long blockNum, String txId) {
		FabricTransaction fabTransObj = new FabricTransaction();
		try {
			checkConfig();
			Channel channel = reconstructChannel();
			String channelName = channel.getName();
			BlockInfo returnedBlock = channel.queryBlockByNumber(blockNum);

			for (BlockInfo.EnvelopeInfo envelopeInfo : returnedBlock.getEnvelopeInfos()) {
				if (envelopeInfo.getTransactionID().equalsIgnoreCase(txId)) {
					fabTransObj.setTransaction_id(envelopeInfo.getTransactionID());
					fabTransObj.setChannel_id(envelopeInfo.getChannelId());
					if (envelopeInfo.getType() == TRANSACTION_ENVELOPE) {
						BlockInfo.TransactionEnvelopeInfo transactionEnvelopeInfo = (BlockInfo.TransactionEnvelopeInfo) envelopeInfo;
						for (BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo transactionActionInfo : transactionEnvelopeInfo
								.getTransactionActionInfos()) {
							fabTransObj.setTransaction_status(transactionActionInfo.getResponseStatus());
							String input_args = "";
							for (int z = 0; z < transactionActionInfo.getChaincodeInputArgsCount(); ++z) {
								input_args += printableString(
										new String(transactionActionInfo.getChaincodeInputArgs(z), "UTF-8")) + " ";
							}
							fabTransObj.setTransaction_args(input_args);
							TxReadWriteSetInfo rwsetInfo = transactionActionInfo.getTxReadWriteSet();
							if (null != rwsetInfo) {
								String readKey = "";
								String writekey = "";
								String chaincodeID = "";
								for (TxReadWriteSetInfo.NsRwsetInfo nsRwsetInfo : rwsetInfo.getNsRwsetInfos()) {
									KvRwset.KVRWSet rws = nsRwsetInfo.getRwset();

									for (KvRwset.KVRead readList : rws.getReadsList()) {
										if (chaincodeID == "")
											chaincodeID = readList.getKey();
										else
											readKey += readList.getKey() + " ";
									}
									for (KvRwset.KVWrite writeList : rws.getWritesList()) {
										String valAsString = printableString(
												new String(writeList.getValue().toByteArray(), "UTF-8"));
										writekey += writeList.getKey() + "=" + valAsString + " ";
									}
								}
								fabTransObj.setChaincode_name(chaincodeID);
								fabTransObj.setTrans_read_key(readKey);
								fabTransObj.setTrans_write_key(writekey);
							}
							for (int i = 0; i < transactionActionInfo.getEndorsementsCount(); i++) {
								EndorserInfo endorserObj = transactionActionInfo.getEndorsementInfo(i);
								Identities.SerializedIdentity endorser = Identities.SerializedIdentity
										.parseFrom(endorserObj.getEndorser());
								fabTransObj.setEndorser_id(endorser.getMspid());
							}
						}
					}
				} else
					continue;

			}
		} catch (Exception e) {
			logger.error("QueryTransactionWithBlkID | QueryBlockWithId | " + e.getMessage());
		}
		return fabTransObj;
	}

}