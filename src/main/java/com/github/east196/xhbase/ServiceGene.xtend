package com.github.east196.xhbase

import com.google.common.base.CaseFormat
import com.google.common.base.Charsets
import com.google.common.io.Files
import java.io.File
import java.util.List
import org.boon.Boon
import org.boon.IO
import org.eclipse.xtend.lib.annotations.Accessors
import org.eclipse.xtend.lib.annotations.EqualsHashCode
import org.eclipse.xtend.lib.annotations.ToString

class ServiceGene {
	
	static def copy(CharSequence content, String path) {
		val file = new File(path)
		Files.createParentDirs(file);
		Files.write(content, file, Charsets.UTF_8)
	}
	
	static def toUpperPreffix(String javaField) {
		CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE,javaField)
	}
	
	def static void main(String[] args) {
		var projectPath = '''E:\workspace\github\east196\java\xhbase\'''
		val basePath = projectPath+'''src\main\java\com\github\east196\hbase'''
		val basePackageName="com.github.east196.hbase"
		
		val tables=Boon.fromJsonArray(IO.read(new File(projectPath+'''src\main\resources\hbase.json''')),MetaTable)
		tables.forEach[table|
			println(Boon.toPrettyJson(table))
		]
		tables.forEach[table|
			var content=bean(basePackageName,table)
			var klassType=table.klassType
			var path='''«basePath»\«klassType».java'''
			copy(content,path)
			println('''«klassType».java''')
//			println(content)
		]
		tables.forEach[table|
			var content=dao(basePackageName,table)
			var klassType=table.klassType
			var path='''«basePath»\«klassType»Dao.java'''
			copy(content,path)
			println('''«klassType»Dao.java''')
//			println(content)
		]
		val hbasemeta=hbasemeta(tables)
		copy(hbasemeta,projectPath+'''src\main\resources\hbasemeta.txt''')
		
	}
	
	def static  String bean(String basePackageName,MetaTable table){
		val items=table.rowKey.items
		val fields = table.cfs.map[cf|
			cf.cqs.map[cq|
				cq.cfName=cf.name
				cq.cfDesc=cf.desc
				return cq
			]
		].flatten.toList
		var klassType=table.klassType
		'''
		package «basePackageName»;
		
		import org.apache.commons.lang3.builder.HashCodeBuilder;
		import org.apache.commons.lang3.builder.EqualsBuilder;
		import org.apache.commons.lang3.builder.ToStringBuilder;
		import org.apache.commons.lang3.builder.ToStringStyle;
		
		public class «klassType» {//«table.klassDesc»
			
			«FOR f : fields»
			«IF f.name!=f.javaName»@SerializedName("«f.name»")«ENDIF»
			private «f.javaType» «f.javaName»;//«f.comment»
			
			«ENDFOR»
			public «klassType»(){}
			
			public «klassType»(«fields.map[it.javaType+" "+it.javaName].join(",")»){
				«FOR f : fields»
				this.«f.javaName»=«f.javaName»;
				«ENDFOR»
			}
		
			public String toRowKey() {
				StringBuilder sb=new StringBuilder();
				«FOR item : items»
				sb.append(«IF item.func.isNullOrEmpty»«item.name»«ENDIF»«IF !item.func.isNullOrEmpty»HbaseFunc.«item.func»(«item.name»)«ENDIF»);
				«IF items.indexOf(item)!=items.size()-1»
				sb.append("_");
				«ENDIF»
				«ENDFOR»
				return sb.toString();
			}
			
			«FOR f : fields»
			public «f.javaType» get«f.javaName.toFirstUpper»() {
				return «f.javaName»;
			}

			public void set«f.javaName.toFirstUpper»(«f.javaType» «f.javaName») {
				this.«f.javaName» = «f.javaName»;
			}
			
			«ENDFOR»
			@Override 
			public int hashCode() {
				return HashCodeBuilder.reflectionHashCode(this);
			}
		
			@Override 
			public boolean equals(Object other) {
				return EqualsBuilder.reflectionEquals(this, other);
			}

			@Override 
			public String toString() {
				return ToStringBuilder.reflectionToString(this,ToStringStyle.DEFAULT_STYLE);
			}
			
		}
		'''
	}
	
	def static  String dao(String basePackageName,MetaTable table){
		val fams = table.cfs
		val cols = table.cfs.map[cf|
			cf.cqs.map[cq|
				cq.cfName=cf.name
				cq.cfDesc=cf.desc
				return cq
			]
		].flatten.toList
		var klassType=table.klassType
		'''
		package «basePackageName»;
		
		import java.util.List;
		
		import org.apache.hadoop.hbase.client.Scan;
		import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
		import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
		import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
		import org.apache.hadoop.hbase.util.Bytes;
		import org.springframework.data.hadoop.hbase.HbaseTemplate;
		import org.springframework.data.hadoop.hbase.RowMapper;
		
		import com.google.common.collect.HashBasedTable;
		import com.google.common.collect.Table;
		
		public class «klassType»Dao {
			
			public static final byte[] «table.name.toUpperPreffix»_TABLE=Bytes.toBytes("«table.name»");
			
			«FOR fam : fams»
			public static final byte[] «fam.name.toUpperPreffix»_FAM=Bytes.toBytes("«fam.name»");
			
			«ENDFOR»
			«FOR col : cols»
			public static final byte[] «col.javaName.toUpperPreffix»_COL_«col.cfName.toUpperPreffix»_FAM=Bytes.toBytes("«col.javaName»");
			
			«ENDFOR»
			public static RowMapper<«klassType»> «table.name»RowMapper;
			
			static {
				«table.name»RowMapper = (result, rowNum) -> {
					«klassType» «table.name» = new «klassType»();
					«FOR col : cols»
					«table.name».set«col.javaName.toFirstUpper»(Bytes.to«col.javaType.toFirstUpper»(result.getValue(«col.cfName.toUpperPreffix»_FAM, «col.javaName.toUpperPreffix»_COL_«col.cfName.toUpperPreffix»_FAM)));
					«ENDFOR»
					return «table.name»;
				};
			}
			
			private HbaseTemplate hbaseTemplate;
		
			public «klassType»Dao(HbaseTemplate hbaseTemplate) {
				this.hbaseTemplate = hbaseTemplate;
			}
			
			public void put(«klassType» «table.name») {
				put(«table.name».toRowKey(), «table.name»);
			}
			
			public void put(String rowKey, «klassType» «table.name») {
				Table<String, String, byte[]> rowFamilyTable = HashBasedTable.create();
				«FOR col : cols»
				rowFamilyTable.put("«col.cfName»","«col.javaName»", Bytes.toBytes(«table.name».get«col.javaName.toFirstUpper»()));
				«ENDFOR»
				hbaseTemplate.put("«table.name»", rowKey, rowFamilyTable);
			}
			
			public «klassType» get(«klassType» «table.name») {
				return get(«table.name».toRowKey());
			}
			
			public «klassType» get(String rowKey) {
				return hbaseTemplate.get("«table.name»", rowKey, «table.name»RowMapper);
			}
			
			public List<«klassType»> find(Scan scan) {
				return hbaseTemplate.find("«table.name»", scan, «table.name»RowMapper);
			}
			
			public void delete(«klassType» «table.name») {
				delete(«table.name».toRowKey());
			}
			
			public void delete(String rowKey) {
				hbaseTemplate.delete("«table.name»", rowKey);
			}
			
			public Long count(){
				Scan scan = new Scan();
				scan.setFilter(new KeyOnlyFilter());
				scan.setMaxVersions();
				return count(scan);
			}
			
			public Long count(Scan scan){
				return hbaseTemplate.execute("«table.name»", table->{
					long count = 0;
					try (AggregationClient ac = new AggregationClient(table.getConfiguration());){
						count= ac.rowCount(table, new LongColumnInterpreter(), scan);
					} catch (Throwable e) {
						e.printStackTrace();
					}
					return count;
				});
			}

		}
		'''
	}
	
		def static  String hbasemeta(List<MetaTable> tables){
		'''
HBaseMeta

«FOR table : tables»	
«tables.indexOf(table)+1». «table.name» -- «table.desc»	
(0)新建指定表				hbase> create '«table.name»', «table.cfs.map["'"+it.name+"'"].join(", ")»		
(1)disable指定表			hbase> disable '«table.name»'
(2)添加aggregation 	hbase> alter '«table.name»', METHOD => 'table_att','coprocessor'=>'|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'
(3)重启指定表 				hbase> enable '«table.name»'

«ENDFOR»
		'''
	}

	@Accessors
	@EqualsHashCode
	@ToString
	static class MetaTable {
		String name
		String desc
		MetaRowKey rowKey
		List<MetaColFamily> cfs
		
		def klassType(){
			name.toFirstUpper
		}
	
		def  klassDesc() {
			desc?:name
		}
	
	}
	
	@Accessors
	@EqualsHashCode
	@ToString
	static class MetaRowKey {
		String splitter
		List<MetaRowKeyItem> items
	}
	
	@Accessors
	@EqualsHashCode
	@ToString
	static class MetaRowKeyItem {
		String name
		String func
	}

	@Accessors
	@EqualsHashCode
	@ToString
	static class MetaColFamily {
		String name
		String desc
		List<MetaColQulifier> cqs
	}

	@Accessors
	@EqualsHashCode
	@ToString
	static class MetaColQulifier {
		String name
		String type
		String desc
		String cfName
		String cfDesc
	
		def  javaName() {
			name
		}
	
		def  javaType() {
			type?:"String"
		}
		
		def comment() {
			desc?:name
		}
		
		def cfComment() {
			cfDesc?:cfName
		}
	
	}
}
