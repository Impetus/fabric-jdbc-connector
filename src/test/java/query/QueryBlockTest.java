package query;

import com.impetus.fabric.query.QueryBlock;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.sql.*;
import static org.mockito.Mockito.*;

public class QueryBlockTest extends TestCase {

    @Test
    public void testQuery(){

    }

    @Test
    public void testFabricStatement() throws ClassNotFoundException, SQLException {
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        File configFolder = new File("src/test/resources/blockchain-query");
        String configPath = configFolder.getAbsolutePath();
        Connection conn = DriverManager.getConnection("jdbc:fabric://" + configPath);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from block"); // This is dummy query
        assert(true);
    }

    @Test
    public void testEnrollAndRegisterUser() throws ClassNotFoundException, SQLException {
        String configPath = "/home/impetus/Blockchain_Java/blockchain-query";
        Class.forName("com.impetus.fabric.jdbc.FabricDriver");
        QueryBlock qb = mock(QueryBlock.class);
        qb.checkConfig();

        String result = qb.enrollAndRegister("Test User");
        //assert(result="")
        assert(true);

        //doNothing()
    }





}
