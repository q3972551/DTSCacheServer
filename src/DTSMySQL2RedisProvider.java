import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aliyun.drc.client.message.ByteString;
import com.aliyun.drc.client.message.DataMessage.Record;
import com.aliyun.drc.client.message.DataMessage.Record.Field;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * MySQL 地理空间类型,WKB ---> WKT
 * <dependency> <groupId>com.vividsolutions</groupId>
 * <artifactId>jts</artifactId>
 * <version>1.13</version> </dependency>
 */

/**
 * <dependency> 
 * <groupId>redis.clients</groupId> 
 * <artifactId>jedis</artifactId>
 * <version>2.7.2</version>
 * </dependency>
 */

public class DTSMySQL2RedisProvider {

	public  static final  String accessKey                = "D0BFB2h77HrSBfXI";
	public  static final  String accessSecret             = "lOkuV3q6ZI2NOgdhN5USex43GkPbXM";
	public  static final  String Subscription_Instance_ID = "dtsskkjuid9q3w7";
	private static final  String redisPassword            = "Hu895623";
	private static final  int    redisPort                = 6379;
	private static final String  redisUrl                 = "47.94.44.109";

	private static  DTSMySQL2RedisProvider s_instance    = null;
	// 连接池
	private JedisPool pool = null;
	private DTSMySQL2RedisProvider()
	{
		
	}
	
	public static DTSMySQL2RedisProvider getInstance()
	{
		if (s_instance == null )
		{
			s_instance = new DTSMySQL2RedisProvider();
		}
		
		return s_instance;
	}
	// byte 数组转换为对应的16进制字符串.
	private char[] HEXCHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	private String toHexString(byte[] bytes) {
		if (null == bytes || bytes.length <= 0) {
			return null;
		}
		char[] hexChars = new char[bytes.length * 2];
		int index = 0;
		for (byte b : bytes) {
			hexChars[index++] = HEXCHARS[b >>> 4 & 0xf];
			hexChars[index++] = HEXCHARS[b & 0xf];
		}
		return new String(hexChars);
	}



	private Jedis getJedisClient() 
	{
		if (null == pool) 
		{
			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxIdle(200);
			config.setMaxWaitMillis(5* 1000);
			config.setMaxTotal(15);
			config.setTestOnBorrow(true);
			config.setTestOnReturn(true);
			pool = new JedisPool(config,redisUrl,redisPort,3000,redisPassword);
		}
		return pool.getResource();
	}

	public String convert2Redis(Field f) throws NumberFormatException, UnsupportedEncodingException {
		ByteString value = f.getValue();
		if (null == value || f.getType() == Record.Field.Type.NULL) {
			return null;
		}
		String string = null;
		if (f.getType() == Record.Field.Type.BIT) {
			if (f.length <= 1) {

				// Bit 1 对应Boolean 类型
				string = Boolean.toString(value.toString("UTF-8").equals("1"));
			} else {
				// Bit[2-64] Binary data
				string = toHexString(value.getBytes());
			}
		} else if (f.getType() == Record.Field.Type.BLOB) {

			// Binary data
			string = toHexString(value.getBytes());

		} else if (f.getType() == Record.Field.Type.GEOMETRY) {
			byte[] tmp = value.getBytes();

			if (null == tmp || tmp.length < 4) {
				return null;
			}
			byte[] wkb = new byte[tmp.length - 4];
			System.arraycopy(tmp, 4, wkb, 0, wkb.length);
			
			GeometryFactory geometryFactory = new GeometryFactory();
			WKBReader reader = new WKBReader(geometryFactory);
			Geometry geometry = null;
			try {
				geometry = reader.read(value.getBytes());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			string = null == geometry ? null : geometry.toText();

		} else if (f.getType() == Record.Field.Type.STRING || f.getType() == Record.Field.Type.INT8
				|| f.getType() == Record.Field.Type.INT16 || f.getType() == Record.Field.Type.INT24
				|| f.getType() == Record.Field.Type.INT32 || f.getType() == Record.Field.Type.INT64
				|| f.getType() == Record.Field.Type.DECIMAL || f.getType() == Record.Field.Type.FLOAT
				|| f.getType() == Record.Field.Type.DOUBLE || f.getType() == Record.Field.Type.TIME
				|| f.getType() == Record.Field.Type.YEAR || f.getType() == Record.Field.Type.DATE
				|| f.getType() == Record.Field.Type.DATETIME || f.getType() == Record.Field.Type.TIMESTAMP
				|| f.getType() == Record.Field.Type.SET || f.getType() == Record.Field.Type.ENUM) {

			// 数字,字符,时间类型
			string = value.toString("UTF-8");
		}
		return string;
	}

	public String createKey(Record record, int start, int step) {

		int count = record.getFieldCount();
		List<Field> fields = record.getFieldList();
		StringBuilder key = new StringBuilder();
		String tbName = record.getTablename();
		key.append(tbName);
		int keyFillCount = 0;

		try {
			// 根据主键值生成 key
			for (int i = start; i < count; ++i) {
				Field f = fields.get(i);
				if (f.isPrimary()) {
					key.append(":").append(convert2Redis(f));
					keyFillCount += 1;
				}
			}
		} catch (UnsupportedEncodingException ue) {
			// 抛出异常，需要处理

			ue.printStackTrace();
		} catch (NumberFormatException ne) {
			// 抛出异常，需要处理

			ne.printStackTrace();
		}

		if (keyFillCount > 0) {
			return key.toString();
		}
		try {
			// 无主键表，根据传入字段生成Key.

			Set<String> keyNameSet = getKeyNameSet(tbName);
			for (int i = start; i < count; ++i) {
				Field f = fields.get(i);
				if (keyNameSet.contains(f.name)) {
					key.append(":").append(convert2Redis(f));
					keyFillCount += 1;
				}
			}
		} catch (UnsupportedEncodingException ue) {
			// 抛出异常，需要处理
			ue.printStackTrace();
		} catch (NumberFormatException ne) {
			// 抛出异常，需要处理
			ne.printStackTrace();
		}

		if (keyFillCount > 0) {
			return key.toString();
		}

		try {
			// 无主键表，未传入任何有效的字段名称
			for (int i = start; i < count; ++i) {
				Field f = fields.get(i);
				key.append(":").append(convert2Redis(f));
				keyFillCount += 1;
			}
		} catch (UnsupportedEncodingException ue) {
			// 抛出异常，需要处理

			ue.printStackTrace();
		} catch (NumberFormatException ne) {
			// 抛出异常，需要处理
			ne.printStackTrace();
		}
		return key.toString();
	}

	// 可根据指定字段生成Key值，针对无主键表有效
	public Set<String> getKeyNameSet(String tbName) {

		return new HashSet<String>();
	}

	// 解析所有字段，获取Map ==> Redis hash 类型
	public Map<String, String> parseFieldValues(Record record, int start, int step)
	{
		if (null == record)
		{
			return null;
		}
		
		Map<String, String> fieldValues = new HashMap<String, String>();
		List<Field> fields = record.getFieldList();
		int count = fields.size();
		try {
			for (int i = start; i < count; i += step) 
			{
				Field f = fields.get(i);
				String value = convert2Redis(f);
				if (null != value)
				{
					fieldValues.put(f.name, value);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fieldValues.isEmpty() ? null : fieldValues;
	}

	// 删除一条数据
	public void replicateDeleteRecord(String key)
	{
		if (null == key) {
			return;
		}
		Jedis jedis = getJedisClient();
		System.out.println("Delete one key=" + key);
		jedis.del(key);
		pool.returnResourceObject(jedis);
	}

	// 插入一条数据
	public void replicateInsertRecord(String key, Map<String, String> hash) 
	{
		if (null == key || null == hash)
		{
			return;
		}
		System.out.println("Insert one key=" + key + " , value=" + hash);
		Jedis jedis = getJedisClient();
		jedis.hmset(key, hash);
		pool.returnResourceObject(jedis);
	}

	// 修改一条数据,
	// 1.删除旧镜像值对应key.
	// 2.插入新镜像值
	public void replicateUpdateRecord(String newKey, Map<String, String> newHash, String oldKey)
	{
		System.out.println("Update one oldKey=" + oldKey + " , newKey=" + newKey + " , value=" + oldKey);
		Jedis jedis = getJedisClient();
		jedis.del(oldKey);
		jedis.hmset(newKey, newHash);
		pool.returnResourceObject(jedis);
	}

	public void replicatorMessage(ClusterMessage message) 
	{
		if (null == message)
		{
			return;
		}
		Record record = message.getRecord();
		if (Record.Type.INSERT == record.getOpt()) 
		{
			// 后镜像值按照依次排列
			Map<String, String> fieldValues = parseFieldValues(record, 0, 1);
			if (null != fieldValues) {
				String key = createKey(record, 0, 1);
				replicateInsertRecord(key, fieldValues);
			}
		}
		else if (Record.Type.UPDATE == record.getOpt())
		{
			// 前后镜像值按照 old,new,old,new 排列

			Map<String, String> fieldValues = parseFieldValues(record, 1, 2);
			if (null != fieldValues) {
				String oldKey = createKey(record, 0, 2);
				String newKey = createKey(record, 1, 2);
				replicateUpdateRecord(newKey, fieldValues, oldKey);
			}
			
		} else if (Record.Type.DELETE == record.getOpt())
		{
			// 前镜像值按照依次排列
			String oldKey = createKey(record, 0, 1);
			replicateDeleteRecord(oldKey);
		} else {
			System.out.println("filter one record op=" + record.getOpt() + " , timestamp=" + record.getTimestamp());
		}
	}
}
