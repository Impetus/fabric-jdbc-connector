package com.impetus.fabric.query;

import com.impetus.fabric.model.Config;
import com.impetus.fabric.model.HyperUser;
import com.impetus.fabric.model.Org;
import com.impetus.fabric.model.Store;
import com.impetus.fabric.query.QueryBlock;
import junit.framework.TestCase;
import org.hyperledger.fabric.sdk.*;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.sql.*;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.powermock.configuration.ConfigurationType.PowerMock;

import org.powermock.modules.junit4.PowerMockRunner;



@RunWith(PowerMockRunner.class)
@PrepareForTest({QueryBlock.class,HFClient.class, SDKUtils.class})
public class QueryBlockTest extends TestCase {


    @Test
    public void testEnrollAndRegisterUser() throws ClassNotFoundException, SQLException, java.lang.Exception {
        String configPath = "src/test/resources/blockchain-query";
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        QueryBlock qb = new QueryBlock(configPath);

        HyperUser mockuser = mock(HyperUser.class);
        when(mockuser.isEnrolled()).thenReturn(true);
        when(mockuser.isRegistered()).thenReturn(true);

        Store mockStore = mock(Store.class);
        when(mockStore.getMember("admin","peerOrg1")).thenReturn(mockuser);
        PowerMockito.whenNew(Store.class).withAnyArguments().thenReturn(mockStore);

        when(mockStore.getMember("abcd1","peerOrg1")).thenReturn(mockuser);

        String result = qb.enrollAndRegister("abcd1");
        assert(result.equals("User  Enrolled Successfuly"));

    }

    @Test
    public void testLoadUserFromPersistence() throws ClassNotFoundException, SQLException{
        String configPath = "src/test/resources/blockchain-query";
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        QueryBlock qb = new QueryBlock(configPath);

        qb.checkConfig();

        String result = qb.loadUserFromPersistence("dummyUser1");
        assert(result.equals("Successfully loaded member from persistence"));
    }

    @Mock
    HFClient mockClient;
    @Test
    public void testReconstructChannel() throws ClassNotFoundException, SQLException, InvalidArgumentException, ProposalException, java.lang.Exception {

        PowerMockito.mockStatic(HFClient.class);
        when(HFClient.createNewInstance()).thenReturn(mockClient);

        Set<String> returnSet = new HashSet<String>();
        returnSet.add("a");
        returnSet.add("c");

        when(mockClient.queryChannels(any(Peer.class))).thenReturn(returnSet);

        String configPath = "src/test/resources/blockchain-query";
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        QueryBlock qb = new QueryBlock(configPath);
        qb.enrollAndRegister("admin");

        Channel mockChannel = mock(Channel.class);
        when(mockClient.newChannel("mychannel")).thenReturn(mockChannel);

        qb.reconstructChannel();

    }

    @Mock
    SDKUtils mockSDKUtils;
    @Test
    public void testInstallChainCode() throws ClassNotFoundException, SQLException, InvalidArgumentException{

        PowerMockito.mockStatic(SDKUtils.class);

        PowerMockito.mockStatic(HFClient.class);
        when(HFClient.createNewInstance()).thenReturn(mockClient);

        Channel mockChannel = mock(Channel.class);
        when(mockClient.newChannel("mychannel")).thenReturn(mockChannel);

        InstallProposalRequest mockInstallProposalRequest = mock(InstallProposalRequest.class);
        when(mockClient.newInstallProposalRequest()).thenReturn(mockInstallProposalRequest);


        String configPath = "src/test/resources/blockchain-query";
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        QueryBlock qb = new QueryBlock(configPath);
        String chaincodeName ="chncodefunc";
        String version = "1.0";
        String goPath = "/home/impetus/IdeaProjects/fabric-jdbc-driver/src/test/resources/blockchain-query/";
        String chaincodePath = "hyperledger/fabric/examples/chaincode/go/chaincode_example02";


        when(mockSDKUtils.getProposalConsistencySets(anyCollection())).thenReturn(new ArrayList());
        String result = qb.installChaincode(chaincodeName, version, qb.getConf().getConfigPath(), chaincodePath);
        assert(result.equals("Chaincode installed successfully"));

    }


    @Test
    public void testInstantiateChaincode() throws ClassNotFoundException, SQLException, InvalidArgumentException{

        PowerMockito.mockStatic(HFClient.class);
        when(HFClient.createNewInstance()).thenReturn(mockClient);

        Channel mockChannel = mock(Channel.class);
        when(mockClient.newChannel("mychannel")).thenReturn(mockChannel);

        InstantiateProposalRequest mockInstantiateProposalRequest = mock(InstantiateProposalRequest.class);
        when(mockClient.newInstantiationProposalRequest()).thenReturn(mockInstantiateProposalRequest);


        String configPath = "src/test/resources/blockchain-query";
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        QueryBlock qb = new QueryBlock(configPath);
        //QueryBlock qb = mock(QueryBlock.class);
        String chaincodeName ="chncodefunc";
        String version = "1.0";
        String goPath = "/home/impetus/IdeaProjects/fabric-jdbc-driver/src/test/resources/blockchain-query/";
        String chaincodePath = "hyperledger/fabric/examples/chaincode/go/chaincode_example02";
        //when(qb.instantiateChaincode(chaincodeName,version,goPath,"testFunction",new String[] {"a","b","5","10"})).thenCallRealMethod();




        String result = qb.instantiateChaincode(chaincodeName,version,goPath,"testFunction",new String[] {"a","b","5","10"});

        assert(result.equals("Chaincode instantiated Successfully"));
    }

}
