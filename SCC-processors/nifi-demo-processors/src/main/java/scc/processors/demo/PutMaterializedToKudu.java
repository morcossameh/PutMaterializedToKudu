/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package scc.processors.demo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteSource;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.security.auth.login.LoginException;
import java.sql.*;

import org.apache.commons.io.Charsets;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.Record;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Configuration;


@EventDriven
@SupportsBatching
@RequiresInstanceClassLoading
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"put", "database", "NoSQL", "kudu", "HDFS", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records to the specified Kudu's table. The schema for the table must be provided in the processor properties or from your source. If any error occurs while reading records from the input, or writing records to Kudu, the FlowFile will be routed to failure")
@WritesAttribute(
    attribute = "record.count",
    description = "Number of records written to Kudu"
)
public class PutMaterializedToKudu extends AbstractProcessor {
    protected static final PropertyDescriptor KUDU_MASTERS;
    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE;
//    protected static final PropertyDescriptor SKIP_HEAD_LINE;
    protected static final PropertyDescriptor FLUSH_MODE;
    protected static final PropertyDescriptor QUERIES;
    protected static final PropertyDescriptor HIVE_URL;
//    protected static final PropertyDescriptor FLOWFILE_BATCH_SIZE;
//    protected static final PropertyDescriptor BATCH_SIZE;
    protected static final Relationship REL_SUCCESS;
    protected static final Relationship REL_FAILURE;
//    public static final String RECORD_COUNT_ATTR = "record.count";
    protected FlushMode flushMode;
    protected int batchSize = 100;
    protected int ffbatch = 1;
    protected KuduClient kuduClient;
    protected KuduTable kuduTable;
    protected String queriesJson;
    protected String hiveConnectionURL;
    private volatile KerberosUser kerberosUser;
    private String tableName = null;
    private Connection conn = null;
    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    public PutMaterializedToKudu() {
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList();
        properties.add(KUDU_MASTERS);
        properties.add(QUERIES);
        properties.add(HIVE_URL);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
//        properties.add(SKIP_HEAD_LINE);
        properties.add(FLUSH_MODE);
//        properties.add(FLOWFILE_BATCH_SIZE);
//        properties.add(BATCH_SIZE);
        return properties;
    }

    public Set<Relationship> getRelationships() {
        Set<Relationship> rels = new HashSet();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException, LoginException {
        String kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        this.queriesJson = context.getProperty(QUERIES).getValue();
        this.hiveConnectionURL = context.getProperty(HIVE_URL).getValue();
//        this.batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
//        this.ffbatch = context.getProperty(FLOWFILE_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        this.flushMode = FlushMode.valueOf(context.getProperty(FLUSH_MODE).getValue());
        this.getLogger().debug("Setting up Kudu connection...");
        this.getLogger().debug(this.queriesJson);
        KerberosCredentialsService credentialsService = (KerberosCredentialsService)context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        this.kuduClient = this.createClient(kuduMasters, credentialsService);
    }

    protected KuduClient createClient(String masters, KerberosCredentialsService credentialsService) throws LoginException {
        if (credentialsService == null) {
            return this.buildClient(masters);
        } else {
            String keytab = credentialsService.getKeytab();
            String principal = credentialsService.getPrincipal();
            this.kerberosUser = this.loginKerberosUser(principal, keytab);
            KerberosAction<KuduClient> kerberosAction = new KerberosAction(this.kerberosUser, () -> {
                return this.buildClient(masters);
            }, this.getLogger());
            return (KuduClient)kerberosAction.execute();
        }
    }

    protected KuduClient buildClient(String masters) {
        return (new KuduClientBuilder(masters)).build();
    }

    protected KerberosUser loginKerberosUser(String principal, String keytab) throws LoginException {
        KerberosUser kerberosUser = new KerberosKeytabUser(principal, keytab);
        kerberosUser.login();
        return kerberosUser;
    }

    @OnStopped
    public final void closeClient() throws KuduException, LoginException {
        try {
            if (this.kuduClient != null) {
                this.getLogger().debug("Closing KuduClient");
                this.kuduClient.close();
                this.kuduClient = null;
            }
        } finally {
            if (this.kerberosUser != null) {
                this.kerberosUser.logout();
                this.kerberosUser = null;
            }

        }

    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(this.ffbatch);
        if (!flowFiles.isEmpty()) {
            KerberosUser user = this.kerberosUser;
            if (user == null) {
                this.trigger(context, session, flowFiles);
            } else {
                PrivilegedExceptionAction<Void> privelegedAction = () -> {
                    this.trigger(context, session, flowFiles);
                    return null;
                };
                KerberosAction<Void> action = new KerberosAction(user, privelegedAction, this.getLogger());
                action.execute();
            }
        }
    }

    private void trigger(ProcessContext context, ProcessSession session, List<FlowFile> flowFiles) throws ProcessException {
    	FlowFile flowFile = flowFiles.get(0);
    	
		try {
	    	String currentTableName = flowFile.getAttribute("database_name");
	    	currentTableName += "::" + flowFile.getAttribute("table_name");
	    	if(!currentTableName.equals(tableName)) {
	    		try {
	    			tableName = currentTableName;
		            this.kuduTable = this.kuduClient.openTable(tableName);
		            this.getLogger().debug("Kudu connection successfully initialized");
	    		} catch(Exception var52) {
	    			this.getLogger().error("Failed to connect", var52);
	    		}
	    	}
	    	KuduSession kuduSession = this.getKuduSession(this.kuduClient);
	    	
	    	Operation operation = this.operationToKudu(flowFile);
	    	
	    	kuduSession.apply(operation);
	    	kuduSession.close();
	    	if (kuduSession.countPendingErrors() != 0) {
	    		this.getLogger().error("errors inserting rows");
	    	    org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = kuduSession.getPendingErrors();
	    	    org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
	    	    int numErrs = errs.length;
	    	    this.getLogger().error("there were errors inserting rows to Kudu");
	    	    this.getLogger().error("the first few errors follow:");
	    	    for (int i = 0; i < numErrs; i++) {
	    	    	this.getLogger().error(errs[i].toString());
	    	    }
	    	    if (roStatus.isOverflowed()) {
	    	    	this.getLogger().error("error buffer overflowed: some errors were discarded");
	    	    }
	    	    session.transfer(flowFiles.get(0), REL_FAILURE);
	    	} else {
	    		session.transfer(flowFiles.get(0), REL_SUCCESS);
	    	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.getLogger().error(e.getMessage());
    	    session.transfer(flowFiles.get(0), REL_FAILURE);
		}
    }
    
    private Operation getJoinRows(String primaryKey, Operation operation, FlowFile flowFile) {
    	try
	    {
	    	String currentDName = flowFile.getAttribute("database_name");
	    	String currentTName = flowFile.getAttribute("table_name");
    		String query = null;
    		int queriesJsonLength = JsonPath.read(this.queriesJson, "$.length()");
	        this.getLogger().debug("queriesJsonLength: " + queriesJsonLength);
    		for(int i = 0; i < queriesJsonLength; i++) {
    			String dName = JsonPath.read(this.queriesJson, "$[" + i + "].dName");
    			if(currentDName.equals(dName)) {
    				String tName = JsonPath.read(this.queriesJson, "$[" + i + "].tName");
    				if(tName.equals(currentTName)) {
    					query = JsonPath.read(this.queriesJson, "$[" + i + "].query");
    					break;
    				}
    			}
    			if(i == queriesJsonLength-1) return null;
    		}
    		
    		Class.forName(JDBC_DRIVER_NAME);
    		
    		String connectionUrl = this.hiveConnectionURL + "/" + currentDName;
//    		String connectionUrl = this.hiveConnectionURL + "jdbc:hive2://localhost:10000/banking";
    		
    		if(conn == null) {
    			conn = DriverManager.getConnection(connectionUrl, "hdfs", "");
    		}
		  
    		// our SQL SELECT query. 
    		// if you only need a few columns, specify them by name instead of using "*"
//    		String query = "select MT_CODE, TRAN_AMOUNT, TERM_ID, CARD_ID, TRAN_SOURCE, TRAN_DEST, RECORD_DATE, card_no, a.name as custName, bSource.name as sourceName, bDest.name as destName from transactions as t inner join cards as c on t.CARD_ID = c.id inner join customers as a on c.cust_id = a.id inner join banks as bSource on bSource.id = t.TRAN_SOURCE inner join banks as bDest on bDest.id = t.TRAN_DEST where t.MT_CODE = " + primaryKey;
//    		String query = "select * from transactions;";
    		
    		query = query.replace("|||", primaryKey);
    		
    		// create the java statement
    		Statement st = conn.createStatement();
		  
    		// execute the query, and get a java resultset
    		ResultSet rs = st.executeQuery(query);
    		
    		rs.next();
    		
    		int columnsCount = rs.getMetaData().getColumnCount();
    		PartialRow row = operation.getRow();
    		for(int i = 1; i <= columnsCount; i++) {
    			String columnName = rs.getMetaData().getColumnLabel(i).toUpperCase();
        		switch(rs.getMetaData().getColumnType(i)) {
        		case Types.INTEGER:
        			row.addInt(columnName, rs.getInt(i));
        			break;
        		case Types.VARCHAR:
        		case Types.CHAR:
        		case Types.LONGVARCHAR:
        			row.addString(columnName, rs.getString(i));
        			break;
        		case Types.TINYINT:
        			row.addByte(columnName, rs.getByte(i));
        			break;
        		case Types.SMALLINT:
        			row.addShort(columnName, rs.getShort(i));
        			break;
        		case Types.BIGINT:
        			row.addLong(columnName, rs.getLong(i));
        			break;
        		case Types.REAL:
        			row.addFloat(columnName, rs.getFloat(i));
        			break;
        		case Types.DOUBLE:
        			row.addDouble(columnName, rs.getDouble(i));
        			break;
        		case Types.DECIMAL:
        			row.addDecimal(columnName, rs.getBigDecimal(i));
        			break;
        		case Types.TIMESTAMP:
        			row.addString(columnName, rs.getTimestamp(i).toString());
        			break;
        		}
        	}
    		st.close();
    		
    		return operation;
    	} catch (Exception e) {
	    	this.getLogger().error("Got an exception! ");
	    	this.getLogger().error(e.getMessage());
	    }
		return null;
    }
    
    private Operation operationToKudu(FlowFile flowFile) {
//    	Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
//    	String type = JsonPath.read(document, "$.type");
//    	List<Integer> columnsTypes = JsonPath.read(document, "$.columns[*].column_type");
//    	List<String> columnsNames = JsonPath.read(document, "$.columns[*].name");
//    	List columnsValues = JsonPath.read(document, "$.columns[*].value");
    	Operation operation = null;
    	String type = flowFile.getAttribute("query_type");
    	String keyValue = flowFile.getAttribute("primary_key");
    	String keyColumnName = flowFile.getAttribute("key_column_name");
    	int keyColumnType = Integer.parseInt(flowFile.getAttribute("key_column_type"));
    	if(type.equals("insert")) {
    		operation = kuduTable.newInsert();
    		return getJoinRows(keyValue, operation, flowFile);
    	} else if(type.equals("delete")) {
    		operation = kuduTable.newDelete();
    	} else if(type.equals("update")) {
    		operation = kuduTable.newUpdate();
    		return getJoinRows(keyValue, operation, flowFile);
    	}
    	PartialRow row = operation.getRow();
    	
		switch(keyColumnType) {
			case Types.INTEGER:
				row.addInt(keyColumnName, Integer.parseInt(keyValue));
				break;
			case Types.VARCHAR:
			case Types.CHAR:
			case Types.LONGVARCHAR:
				row.addString(keyColumnName, keyValue);
				break;
			case Types.TINYINT:
				row.addByte(keyColumnName, Byte.parseByte(keyValue));
				break;
			case Types.SMALLINT:
				row.addShort(keyColumnName, Short.parseShort(keyValue));
				break;
			case Types.BIGINT:
				row.addLong(keyColumnName, Long.parseLong(keyValue));
				break;
			case Types.REAL:
				row.addFloat(keyColumnName, Float.parseFloat(keyValue));
				break;
			case Types.DOUBLE:
				row.addDouble(keyColumnName, Double.parseDouble(keyValue));
				break;
			case Types.DECIMAL:
				row.addDecimal(keyColumnName, new BigDecimal(keyValue));
				break;
		}
    	return operation;
    }

    protected KuduSession getKuduSession(KuduClient client) {
        KuduSession kuduSession = client.newSession();
        kuduSession.setMutationBufferSpace(this.batchSize);
        kuduSession.setFlushMode(this.flushMode);

        return kuduSession;
    }

    private void flushKuduSession(KuduSession kuduSession, boolean close, List<RowError> rowErrors) throws KuduException {
        List<OperationResponse> responses = close ? kuduSession.close() : kuduSession.flush();
        if (kuduSession.getFlushMode() == FlushMode.AUTO_FLUSH_BACKGROUND) {
            rowErrors.addAll(Arrays.asList(kuduSession.getPendingErrors().getRowErrors()));
        } else {
            responses.stream().filter(OperationResponse::hasRowError).map(OperationResponse::getRowError).forEach(rowErrors::add);
        }
    }

    protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
        Upsert upsert = kuduTable.newUpsert();
        this.buildPartialRow(kuduTable.getSchema(), upsert.getRow(), record, fieldNames);
        return upsert;
    }

    protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames) {
        Insert insert = kuduTable.newInsert();
        this.buildPartialRow(kuduTable.getSchema(), insert.getRow(), record, fieldNames);
        return insert;
    }

    @VisibleForTesting
    void buildPartialRow(Schema schema, PartialRow row, Record record, List<String> fieldNames) {
        Iterator var5 = fieldNames.iterator();

        while(var5.hasNext()) {
            String colName = (String)var5.next();
            int colIdx = this.getColumnIndex(schema, colName);
            if (colIdx != -1) {
                ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
                Type colType = colSchema.getType();
                if (record.getValue(colName) == null) {
                    row.setNull(colName);
                } else {
                    switch(colType.getDataType(colSchema.getTypeAttributes())) {
                    case BOOL:
                        row.addBoolean(colIdx, record.getAsBoolean(colName));
                        break;
                    case FLOAT:
                        row.addFloat(colIdx, record.getAsFloat(colName));
                        break;
                    case DOUBLE:
                        row.addDouble(colIdx, record.getAsDouble(colName));
                        break;
                    case BINARY:
                        row.addBinary(colIdx, record.getAsString(colName).getBytes());
                        break;
                    case INT8:
                        row.addByte(colIdx, record.getAsInt(colName).byteValue());
                        break;
                    case INT16:
                        row.addShort(colIdx, record.getAsInt(colName).shortValue());
                        break;
                    case INT32:
                        row.addInt(colIdx, record.getAsInt(colName));
                        break;
                    case INT64:
                    case UNIXTIME_MICROS:
                        row.addLong(colIdx, record.getAsLong(colName));
                        break;
                    case STRING:
                        row.addString(colIdx, record.getAsString(colName));
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        row.addDecimal(colIdx, new BigDecimal(record.getAsString(colName)));
                        break;
                    default:
                        throw new IllegalStateException(String.format("unknown column type %s", colType));
                    }
                }
            }
        }
    }

    private int getColumnIndex(Schema columns, String colName) {
        try {
            return columns.getColumnIndex(colName);
        } catch (Exception var4) {
            return -1;
        }
    }

    static {
        KUDU_MASTERS = (new Builder()).name("Kudu Masters").description("List all kudu masters's ip with port (e.g. 7051), comma separated").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
        KERBEROS_CREDENTIALS_SERVICE = (new Builder()).name("kerberos-credentials-service").displayName("Kerberos Credentials Service").description("Specifies the Kerberos Credentials to use for authentication").required(false).identifiesControllerService(KerberosCredentialsService.class).build();
//        SKIP_HEAD_LINE = (new Builder()).name("Skip head line").description("Deprecated. Used to ignore header lines, but this should be handled by a RecordReader (e.g. \"Treat First Line as Header\" property of CSVReader)").allowableValues(new String[]{"true", "false"}).defaultValue("false").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        FLUSH_MODE = (new Builder()).name("Flush Mode").description("Set the new flush mode for a kudu session.\nAUTO_FLUSH_SYNC: the call returns when the operation is persisted, else it throws an exception.\nAUTO_FLUSH_BACKGROUND: the call returns when the operation has been added to the buffer. This call should normally perform only fast in-memory operations but it may have to wait when the buffer is full and there's another buffer being flushed.\nMANUAL_FLUSH: the call returns when the operation has been added to the buffer, else it throws a KuduException if the buffer is full.").allowableValues(FlushMode.values()).defaultValue(FlushMode.AUTO_FLUSH_BACKGROUND.toString()).required(true).build();
//        FLOWFILE_BATCH_SIZE = (new Builder()).name("FlowFiles per Batch").description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. Depending on your memory size, and data size per row set an appropriate batch size for the number of FlowFiles to process per client connection setup.Gradually increase this number, only if your FlowFiles typically contain a few records.").defaultValue("1").required(true).addValidator(StandardValidators.createLongValidator(1L, 100000L, true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
//        BATCH_SIZE = (new Builder()).name("Batch Size").displayName("Max Records per Batch").description("The maximum number of Records to process in a single Kudu-client batch, between 1 - 100000. Depending on your memory size, and data size per row set an appropriate batch size. Gradually increase this number to find out the best one for best performances.").defaultValue("100").required(true).addValidator(StandardValidators.createLongValidator(1L, 100000L, true)).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
        QUERIES = (new Builder()).name("Queries").displayName("Queries from hive").description("").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
        HIVE_URL = (new Builder()).name("Hive Connection URL").displayName("Hive Connection URL").description("").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
        REL_SUCCESS = (new org.apache.nifi.processor.Relationship.Builder()).name("success").description("A FlowFile is routed to this relationship after it has been successfully stored in Kudu").build();
        REL_FAILURE = (new org.apache.nifi.processor.Relationship.Builder()).name("failure").description("A FlowFile is routed to this relationship if it cannot be sent to Kudu").build();
    }
}
