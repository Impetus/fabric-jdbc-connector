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

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConfigTest {
    
    private static Config config1;
    
    private static Config config2;
    
    @BeforeClass
    public static void beforeClass() {
        String configPath1 = new File("src/test/resources/dummy-config-1").getAbsolutePath();
        String configPath2 = new File("src/test/resources/dummy-config-2").getAbsolutePath();
        config1 = new Config(configPath1);
        config2 = new Config(configPath2);
    }

    @Test
    public void testMultiConfigPath() {
        String configPath1 = new File("src/test/resources/dummy-config-1").getAbsolutePath();
        String configPath2 = new File("src/test/resources/dummy-config-2").getAbsolutePath();
        configPath1 = configPath1.endsWith("/") ? configPath1 : configPath1 + "/";
        configPath2 = configPath2.endsWith("/") ? configPath2 : configPath2 + "/";
        assertEquals(configPath1, config1.getConfigPath());
        assertEquals(configPath2, config2.getConfigPath());
    }
    
    @Test
    public void testMultiConfigOrgs() {
        Org org1 = config1.getSampleOrg();
        assertEquals("Org1MSP", org1.getMSPID());
        Org org2 = config2.getSampleOrg();
        assertEquals("Org2MSP", org2.getMSPID());
    }
    
}
