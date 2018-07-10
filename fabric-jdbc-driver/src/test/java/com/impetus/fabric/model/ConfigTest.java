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
        Org org1 = config1.getSampleOrgs().toArray(new Org[]{})[0];
        assertEquals("Org1MSP", org1.getMSPID());
        Org org2 = config2.getSampleOrgs().toArray(new Org[]{})[0];
        assertEquals("Org2MSP", org2.getMSPID());
    }
    
}
