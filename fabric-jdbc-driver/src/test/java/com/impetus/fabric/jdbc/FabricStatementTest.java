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
package com.impetus.fabric.jdbc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.HFClient;
import org.hyperledger.fabric_ca.sdk.HFCAClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.impetus.blkch.jdbc.BlkchnStatement;
import com.impetus.fabric.model.HyperUser;
import com.impetus.fabric.model.Store;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HFClient.class, HFCAClient.class})
public class FabricStatementTest {
        
    @Mock
    HFCAClient mockCA;
    
    @Mock
    HFClient mockClient;
    
    @Test
    public void testGetSchema() throws Exception {
        PowerMockito.mockStatic(HFCAClient.class);
        when(HFCAClient.createNewInstance(anyString(), any())).thenReturn(mockCA);
        
        PowerMockito.mockStatic(HFClient.class);
        when(HFClient.createNewInstance()).thenReturn(mockClient);
        
        Channel mockChannel = mock(Channel.class);
        when(mockClient.newChannel(anyString())).thenReturn(mockChannel);
        
        HyperUser mockuser = mock(HyperUser.class);
        when(mockuser.isEnrolled()).thenReturn(true);
        Store mockStore = mock(Store.class);
        PowerMockito.whenNew(Store.class).withAnyArguments().thenReturn(mockStore);
        
        String configPath = new File("src/test/resources/blockchain-query").getAbsolutePath();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath + ":mychannel", 
                "admin", "adminpw");
        BlkchnStatement stat = (BlkchnStatement) conn.createStatement();
        String sql = "Select block_no, previous_hash from block";
        ResultSetMetaData metadata = stat.getSchema(sql);
        int columnCount = metadata.getColumnCount();
        assertEquals(2, columnCount);
        assertEquals(metadata.getColumnName(1), "block_no");
        assertEquals(metadata.getColumnName(2), "previous_hash");
        assertEquals(metadata.getColumnType(1), Types.BIGINT);
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);
    }
    
    @Test
    public void testGetSchemaStar() throws Exception {
        PowerMockito.mockStatic(HFCAClient.class);
        when(HFCAClient.createNewInstance(anyString(), any())).thenReturn(mockCA);
        
        PowerMockito.mockStatic(HFClient.class);
        when(HFClient.createNewInstance()).thenReturn(mockClient);
        
        Channel mockChannel = mock(Channel.class);
        when(mockClient.newChannel(anyString())).thenReturn(mockChannel);
        
        HyperUser mockuser = mock(HyperUser.class);
        when(mockuser.isEnrolled()).thenReturn(true);
        Store mockStore = mock(Store.class);
        PowerMockito.whenNew(Store.class).withAnyArguments().thenReturn(mockStore);
        
        String configPath = new File("src/test/resources/blockchain-query").getAbsolutePath();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath + ":mychannel", 
                "admin", "adminpw");
        BlkchnStatement stat = (BlkchnStatement) conn.createStatement();
        String sql = "select * from transaction";
        ResultSetMetaData metadata = stat.getSchema(sql);
        int columnCount = metadata.getColumnCount();
        assertEquals(10, columnCount);
    }
    
    @Test
    public void testArrayElementType() throws Exception {
        PowerMockito.mockStatic(HFCAClient.class);
        when(HFCAClient.createNewInstance(anyString(), any())).thenReturn(mockCA);
        
        PowerMockito.mockStatic(HFClient.class);
        when(HFClient.createNewInstance()).thenReturn(mockClient);
        
        Channel mockChannel = mock(Channel.class);
        when(mockClient.newChannel(anyString())).thenReturn(mockChannel);
        
        HyperUser mockuser = mock(HyperUser.class);
        when(mockuser.isEnrolled()).thenReturn(true);
        Store mockStore = mock(Store.class);
        PowerMockito.whenNew(Store.class).withAnyArguments().thenReturn(mockStore);
        
        String configPath = new File("src/test/resources/blockchain-query").getAbsolutePath();
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath + ":mychannel", 
                "admin", "adminpw");
        BlkchnStatement stat = (BlkchnStatement) conn.createStatement();
        int arrayElementType = stat.getArrayElementType("transaction_action", "chaincode_args");
        assertEquals(Types.VARCHAR, arrayElementType);
    }
}
