package cn.com.yeexun.kuduhelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

/**
 * kudu操作工具类。<br/>
 * 包括CRUD、查询元数据信息、编辑表结构等常用操作。
 * @author yx-hj
 *
 */
public class KuduHelper {
	
	private String kuduMasters;
	
	private KuduClient client;
	
	private KuduTable kuduTable;
	
	public KuduHelper() {
		super();
	}

	public KuduHelper(String kuduMasters) {
		super();
		this.setKuduMasters(kuduMasters);
		this.client = new KuduClient.KuduClientBuilder(kuduMasters)
				.defaultSocketReadTimeoutMs(6000)
				.build();
	}
	
	public KuduHelper(String kuduMasters, String tableName) {
		super();
		this.setKuduMasters(kuduMasters);
		this.client = new KuduClient.KuduClientBuilder(kuduMasters)
				.defaultSocketReadTimeoutMs(6000)
				.build();
		try {
			this.kuduTable = client.openTable(tableName);
		} catch (KuduException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/**
	 * 获取列的元数据信息，包括数据类型，字段名称等信息。
	 * @return
	 */
	public List<ColumnSchema> getColumnSchame() {
        Schema schema = this.kuduTable.getSchema();
        return schema.getColumns();
	}
	
	public List<ColumnSchema> getColumnSchame(String tableName) {
		Schema schema = null;
		if (tableName == null) {
			return this.getColumnSchame();
		} else {
			try {
				schema = client.openTable(tableName).getSchema();
			} catch (KuduException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
	        return schema.getColumns();
		}
	}
	
	/**
	 * 删除指定列。未测试
	 * @param columnName
	 */
	public void dropColumn(String columnName) {
		AlterTableOptions alterTableOptions = new AlterTableOptions();
        
        alterTableOptions.dropColumn(columnName);
        
        try {
			client.alterTable(kuduTable.getName(), alterTableOptions);
		} catch (KuduException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	/**
	 * 添加列。未测试
	 * @param columnName
	 * @param type
	 * @param nullable
	 * @param defaultValue
	 */
	public void addColumn(String columnName,String type, boolean nullable, Object defaultValue) {
		AlterTableOptions alterTableOptions = new AlterTableOptions();
		if (nullable) {
    		alterTableOptions.addNullableColumn(columnName, Type.valueOf(type));
		} else {
			alterTableOptions.addColumn(columnName, Type.valueOf(type), defaultValue);
		}
		try {
			client.alterTable(kuduTable.getName(), alterTableOptions);
		} catch (KuduException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	/**
	 * 查询表中数据。
	 * @param tableName 要查询的表名。
	 * @param limit 要查询的数据的行数。limit &lt; 0 表示查询所有记录。
	 * @return
	 */
	public List<Map<String, Object>> select(String tableName, int limit) {
		
		List<ColumnSchema> columnSchames = getColumnSchame(tableName);
		List<Map<String, Object>> rows = new ArrayList<Map<String,Object>>();
		int rowCount = 0;
		KuduScannerBuilder kuduScannerBuilder = createKuduScannerBuilder(tableName);
		KuduScanner scanner = kuduScannerBuilder.build();
		try {
			while (scanner.hasMoreRows()) {
				RowResultIterator iterator = scanner.nextRows();
				while (iterator.hasNext()) {
					if (limit < 0 || rowCount < limit) {
						RowResult result = iterator.next();
						Map<String, Object> rowData = new HashMap<String, Object>();
						for (int i = 0; i < columnSchames.size(); i++) {
							ColumnSchema columnSchema = columnSchames.get(i);
							Object columnValue = getColumnValue(columnSchema, result);
							rowData.put(columnSchema.getName(), columnValue);
						}
						rows.add(rowData);
						rowCount += 1;
					} else {
						break;
					}
				}
			}
		} catch (KuduException e) {
			throw new RuntimeException(e.getMessage(), e);
		} finally {
			closeScanner(scanner);
		}
		return rows;
	}
	
	public List<Map<String, Object>> select(int limit) {

		return this.select(null, limit);
	}
	
	public void close(KuduClient client) {
		
		try {
			if (client != null) {
				client.close();
			}
		} catch (KuduException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	private void closeScanner(KuduScanner scanner) {
		try {
			if (scanner != null) {
				scanner.close();
			}
		} catch (KuduException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public KuduTable getKuduTable() {
		return kuduTable;
	}

	public void setKuduTable(String tableName) {
		try {
			this.kuduTable = client.openTable(tableName);
		} catch (KuduException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public String getKuduMasters() {
		return kuduMasters;
	}

	public void setKuduMasters(String kuduMasters) {
		this.kuduMasters = kuduMasters;
	}
	
	private Object getColumnValue(ColumnSchema columnSchema, RowResult result) {
		
		Type type = columnSchema.getType();
		Object columnValue = null;
		
		switch (type) {
		case INT8:
			columnValue = result.getByte(columnSchema.getName());
			break;
		case INT16:
			columnValue = result.getShort(columnSchema.getName());
			break;
		case INT32:
			columnValue = result.getInt(columnSchema.getName());
			break;
		case INT64:
		case UNIXTIME_MICROS:
			columnValue = result.getLong(columnSchema.getName());
			break;
		case BINARY:
			columnValue = result.getBinary(columnSchema.getName());
			break;
		case STRING:
			columnValue = result.getString(columnSchema.getName());
			break;
		case BOOL:
			columnValue = result.getBoolean(columnSchema.getName());
			break;
		case FLOAT:
			columnValue = result.getFloat(columnSchema.getName());
			break;
		case DOUBLE:
			columnValue = result.getDouble(columnSchema.getName());
			break;
		default:
			throw new RuntimeException("不支持的数据类型!");
		}
		return columnValue;
	}
	
	private KuduScannerBuilder createKuduScannerBuilder(String tableName) {
		KuduScannerBuilder kuduScannerBuilder = null;
		if (tableName == null) {
			kuduScannerBuilder = client.newScannerBuilder(kuduTable);
		} else {
			try {
				kuduScannerBuilder = client.newScannerBuilder(client.openTable(tableName));
			} catch (KuduException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		return kuduScannerBuilder;
	}
	
	public static void main(String[] args) {
		String kuduMasters = "10.221.121.5:7051";
		KuduHelper kuduHelper = new KuduHelper(kuduMasters, "test_kudu11");
		List<Map<String, Object>> result = kuduHelper.select(-1);
		for (Map<String, Object> map : result) {
			System.out.println(map);
		}
	}
	
}
