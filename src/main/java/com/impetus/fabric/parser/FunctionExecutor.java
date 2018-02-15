package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.function.Args;
import com.impetus.blkch.sql.function.ClassName;
import com.impetus.blkch.sql.function.Parameters;
import com.impetus.blkch.sql.function.Version;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.TreeNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.fabric.query.QueryBlock;

public class FunctionExecutor {
    
    private LogicalPlan logicalPlan;
    
    private QueryBlock queryBlock;
    
    public FunctionExecutor(LogicalPlan logicalPlan, QueryBlock queryBlock) {
        this.logicalPlan = logicalPlan;
        this.queryBlock = queryBlock;
    }

    public void executeCreate() {
        //TODO We need to implement handling of endorsement policy
        TreeNode createFunc = logicalPlan.getCreateFunction();
        String chaincodeName = createFunc.getChildType(IdentifierNode.class, 0).getValue();
        String chaincodePath = createFunc.getChildType(ClassName.class, 0).getName().replaceAll("'", "");
        if(!createFunc.hasChildType(Version.class)) {
            throw new BlkchnException("Version is missing");
        }
        String version = createFunc.getChildType(Version.class, 0).getVersion().replaceAll("'", "");
        List<String> args = new ArrayList<>();
        if(createFunc.hasChildType(Args.class)) {
            Args arguments = createFunc.getChildType(Args.class, 0);
            for(IdentifierNode ident : arguments.getChildType(IdentifierNode.class)) {
                args.add(ident.getValue());
            }
        }
        queryBlock.installChaincode(chaincodeName, version, queryBlock.getConf().getConfigPath(), chaincodePath);
        queryBlock.instantiateChaincode(chaincodeName, version, chaincodePath, "init", args.toArray(new String[]{}));
    }
    
    public DataFrame executeCall() {
        TreeNode callFunc = logicalPlan.getCallFunction();
        String chaincodeName = callFunc.getChildType(IdentifierNode.class, 0).getValue();
        List<String> args = new ArrayList<>();
        if(!callFunc.hasChildType(Parameters.class)) {
            throw new BlkchnException("Invalid number of parameters");
        }
        Parameters params = callFunc.getChildType(Parameters.class, 0);
        List<IdentifierNode> idents = params.getChildType(IdentifierNode.class);
        if(idents.size() == 0) {
            throw new BlkchnException("Invalid number of parameters");
        }
        for(IdentifierNode ident : idents) {
            args.add(ident.getValue());
        }
        String result = queryBlock.queryChaincode(chaincodeName, args.get(0), args.stream().skip(1).collect(Collectors.toList()).toArray(new String[]{}));
        AssetSchema assetSchema = AssetSchema.getAssetSchema(queryBlock.getConf(), chaincodeName, args.get(0));
        DataFrame df = assetSchema.createDataFrame(result);
        df.show();
        return df;
        /*List<List<Object>> data = new ArrayList<>();
        data.add(Arrays.asList(result));
        return new DataFrame(data, Arrays.asList("data"), new HashMap<>());*/
        
    }
}
