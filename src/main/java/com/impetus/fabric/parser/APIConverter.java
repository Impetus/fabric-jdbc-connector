package com.impetus.fabric.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.hyperledger.fabric.sdk.BlockInfo;
import org.hyperledger.fabric.sdk.BlockchainInfo;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FilterItem;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.SelectClause;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.sql.query.WhereClause;
import com.impetus.fabric.query.QueryBlock;

public class APIConverter {
	
	private LogicalPlan logicalPlan;
	
	private QueryBlock queryBlock;
	
	private List<SelectItem> selectItems = new ArrayList<>();
	
	private static final String[] filterableCols = {"blockNo", "blockHash", "transactionId"};
	
	private Map<String, String> aliasMapping = new HashMap<>();

	public APIConverter(LogicalPlan logicalPlan, QueryBlock queryBlock) {
		this.logicalPlan = logicalPlan;
		this.queryBlock = queryBlock;
		SelectClause selectClause = logicalPlan.getQuery().getChildType(SelectClause.class, 0);
		List<SelectItem> selItems = selectClause.getChildType(SelectItem.class);
		for(SelectItem selItem : selItems) {
			selectItems.add(selItem);
			if(selItem.hasChildType(IdentifierNode.class) && selItem.hasChildType(Column.class)) {
				String alias = selItem.getChildType(IdentifierNode.class, 0).getValue();
				String colName = selItem.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
				if(aliasMapping.containsKey(alias)) {
					throw new RuntimeException("Alias " + alias + " is ambiguous");
				} else {
					aliasMapping.put(alias, colName);
				}
			}
		}
	}
	
	public DataFrame executeQuery() {
		List<BlockInfo> blockInfos = getFromTable();
		DataFrame dataframe;
		try {
			dataframe = blockToDataFrame(blockInfos);
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
		return dataframe.select(selectItems);
	}
	
	public List<BlockInfo> getFromTable() {
		List<BlockInfo> blockInfos = new ArrayList<>();
		FromItem fromItem = logicalPlan.getQuery().getChildType(FromItem.class, 0);
		Table table = fromItem.getChildType(Table.class, 0);
		String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
		if("block".equalsIgnoreCase(tableName)) {
			if(logicalPlan.getQuery().hasChildType(WhereClause.class)) {
				blockInfos.addAll(executeWithWhereClause(tableName));
			} else {
				Channel channel = queryBlock.reconstructChannel();
				BlockchainInfo channelInfo;
				try {
					channelInfo = channel.queryBlockchainInfo();
					for(long i = 1 ; i < channelInfo.getHeight() ; i++) {
						blockInfos.add(channel.queryBlockByNumber(i));
					}
				} catch (ProposalException | InvalidArgumentException e) {
					throw new RuntimeException(e);
				}
			}
		} else {
			throw new RuntimeException("Unidentified table " + tableName);
		}
		return blockInfos;
	}
	
	public List<BlockInfo> executeWithWhereClause(String tableName) {
		WhereClause whereClause = logicalPlan.getQuery().getChildType(WhereClause.class, 0);
		if(whereClause.hasChildType(FilterItem.class)) {
			FilterItem filterItem = whereClause.getChildType(FilterItem.class, 0);
			return executeSingleWhereClause(tableName, filterItem);
		} else {
			return executeMultipleWhereClause(tableName, whereClause);
		}
	}
	
	public List<BlockInfo> executeSingleWhereClause(String tableName, FilterItem filterItem) {
		String filterColumn = null;
		if(filterItem.hasChildType(Column.class)) {
			String colName = filterItem.getChildType(Column.class, 0).getName();
			boolean colIdentified = false;
			for(String filterableCol : filterableCols) {
				if(filterableCol.equalsIgnoreCase(colName)) {
					filterColumn = colName;
					colIdentified = true;
					break;
				}
			}
			if(!colIdentified) {
				String colOriginalName = aliasMapping.get(colName);
				for(String filterableCol : filterableCols) {
					if(filterableCol.equalsIgnoreCase(colOriginalName)) {
						filterColumn = colOriginalName;
						colIdentified = true;
						break;
					}
				}
				if(!colIdentified) {
					throw new RuntimeException("Column " + colName + " is not filterable column");
				}
			}
		} else {
			//TODO
			filterColumn = "";
		}
		//TODO Implement comparator function to take other operators(for now only =)
		String value = filterItem.getChildType(IdentifierNode.class, 0).getValue();
		Channel channel = queryBlock.reconstructChannel();
		BlockInfo blockInfo;
		if("blockNo".equalsIgnoreCase(filterColumn)) {
			try {
				blockInfo = channel.queryBlockByNumber(Long.parseLong(value));
			} catch (NumberFormatException | InvalidArgumentException
					| ProposalException e) {
				throw new RuntimeException(e);
			}
		} else if("blockHash".equalsIgnoreCase(filterColumn)) {
			try {
				blockInfo = channel.queryBlockByHash(Hex.decodeHex(value.replace("'", "").toCharArray()));
			} catch (InvalidArgumentException | ProposalException | DecoderException e) {
				throw new RuntimeException(e);
			}
		} else {
			try {
				blockInfo = channel.queryBlockByTransactionID(value.replace("'", ""));
			} catch (InvalidArgumentException | ProposalException e) {
				throw new RuntimeException(e);
			}
		}
		
		return Arrays.asList(blockInfo);
	}
	
	public List<BlockInfo> executeMultipleWhereClause(String tableName, WhereClause whereClause) {
		LogicalOperation operation = whereClause.getChildType(LogicalOperation.class, 0);
		return executeLogicalOperation(tableName, operation);
	}
	
	public List<BlockInfo> executeLogicalOperation(String tableName, LogicalOperation operation) {
		if(operation.getChildNodes().size() != 2) {
			throw new RuntimeException("Logical operation should have two boolean expressions");
		}
		List<BlockInfo> firstBlock, secondBlock;
		if(operation.getChildNode(0) instanceof LogicalOperation) {
			firstBlock = executeLogicalOperation(tableName, (LogicalOperation)operation.getChildNode(0));
		} else {
			FilterItem filterItem = (FilterItem) operation.getChildNode(0);
			firstBlock = executeSingleWhereClause(tableName, filterItem);
		}
		if(operation.getChildNode(1) instanceof LogicalOperation) {
			secondBlock = executeLogicalOperation(tableName, (LogicalOperation)operation.getChildNode(1));
		} else {
			FilterItem filterItem = (FilterItem) operation.getChildNode(1);
			secondBlock = executeSingleWhereClause(tableName, filterItem);
		}
		List<BlockInfo> returnBlocks = new ArrayList<>();
		Map<String, BlockInfo> firstBlockMap = new HashMap<String, BlockInfo>(), secondBlockMap = new HashMap<String, BlockInfo>();
		for(BlockInfo blockInfo : firstBlock) {
			firstBlockMap.put(Hex.encodeHexString(blockInfo.getPreviousHash()), blockInfo);
		}
		for(BlockInfo blockInfo : secondBlock) {
			secondBlockMap.put(Hex.encodeHexString(blockInfo.getPreviousHash()), blockInfo);
		}
		if(operation.isAnd()) {
			for(String previousHash : firstBlockMap.keySet()) {
				if(secondBlockMap.containsKey(previousHash)) {
					returnBlocks.add(secondBlockMap.get(previousHash));
				}
			}
		} else {
			returnBlocks.addAll(firstBlock);
			for(String previousHash : secondBlockMap.keySet()) {
				if(!firstBlockMap.containsKey(previousHash)) {
					returnBlocks.add(secondBlockMap.get(previousHash));
				}
			}
		}
		return returnBlocks;
	}
	
	public DataFrame blockToDataFrame(List<BlockInfo> blockInfos) throws InvalidProtocolBufferException {
		String[] columns = {"previousHash", "blockHash", "transActionsMetaData", "transactionCount",
							"blockNo", "channelId", "transactionId", "transactionType", "timestamp"};
		List<List<Object>> data = new ArrayList<>();
		for(BlockInfo blockInfo : blockInfos) {
			String previousHash = Hex.encodeHexString(blockInfo.getPreviousHash());
			String dataHash = Hex.encodeHexString(blockInfo.getDataHash());
			String transActionsMetaData = Hex.encodeHexString(blockInfo.getTransActionsMetaData());
			int transactionCount = blockInfo.getEnvelopeCount();
			long blockNum = blockInfo.getBlockNumber();
			String channelId = blockInfo.getChannelId();
			for(BlockInfo.EnvelopeInfo envelopeInfo : blockInfo.getEnvelopeInfos()) {
				List<Object> record = new ArrayList<>();
				record.add(previousHash);
				record.add(dataHash);
				record.add(transActionsMetaData);
				record.add(transactionCount);
				record.add(blockNum);
				record.add(channelId);
				record.add(envelopeInfo.getTransactionID());
				record.add(envelopeInfo.getType().toString());
				record.add(envelopeInfo.getTimestamp());
				data.add(record);
			}
		}
		return new DataFrame(data, columns, aliasMapping);
	}

}
