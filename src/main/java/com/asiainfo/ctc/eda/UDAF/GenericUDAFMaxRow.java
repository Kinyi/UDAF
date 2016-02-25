package com.asiainfo.ctc.eda.UDAF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

 
/**
 * The maxrow() aggregate function is similar to the built-in max() function,
 * but it allows you to refer to additional columns in the maximal row.
 * 
 * Usage: 1)SELECT id, maxrow(ts, somedata) FROM sometable GROUP BY id;
 * 		  2)SELECT id, m.col0 as ts, m.col1 as somedata FROM (
 *        		SELECT id, maxrow(ts, somedata) as m FROM sometable GROUP BY id
 *    		) s;
 * 
 * Note: 第一个参数需要是可比较的数据类型,通过第一个参数比较得出最大的一行分组数据,将maxrow函数的所有参数都存放到一个struct类型中
 * 
 * @author Kinyi_Chan
 *
 */

@Description(name = "maxrow", value = "_FUNC_(expr) - Returns the maximum value of expr and values of associated columns as a struct")
public class GenericUDAFMaxRow extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(GenericUDAFMaxRow.class.getName());

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		// Verify that the first parameter supports comparisons.
		ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
		if (!ObjectInspectorUtils.compareSupported(oi)) {
			throw new UDFArgumentTypeException(0, "Cannot support comparison of map<> type or complex type containing map<>.");
		}
		return new GenericUDAFMaxRowEvaluator();
	}

	// @UDFType(distinctLike=true)
	public static class GenericUDAFMaxRowEvaluator extends GenericUDAFEvaluator {

		// 我认为这两个数组的作用是：记录输入的参数列的ObjectInspector类型，后续可以用数组中任意一个作为求最大值的列
		// hadoop的数据类型
		ObjectInspector[] inputOIs;
		// hadoop数据类型所对应的standardOI
		ObjectInspector[] outputOIs;
		// 返回值
		ObjectInspector structOI;

		// 初始化方法，在Mode的每一个阶段启动时会执行init方法
		// 每个阶段parameters数组的长度都可能不一致
		// 这个方法返回了UDAF的返回类型
		@Override
		public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
			super.init(mode, parameters);

			int length = parameters.length;
			if (length > 1 || !(parameters[0] instanceof StructObjectInspector)) {
				// assert(mode == Mode.COMPLETE || mode == Mode.FINAL); //作者的写法
				assert (mode == Mode.COMPLETE || mode == Mode.PARTIAL1); // 我的理解
				initMapSide(parameters);

			} else {
				// assert(mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2);//作者的写法
				assert (mode == Mode.FINAL || mode == Mode.PARTIAL2); // 我的理解
				assert (parameters.length == 1 && parameters[0] instanceof StructObjectInspector);
				initReduceSide((StructObjectInspector) parameters[0]);
			}

			return structOI;
		}

		/* Initialize the UDAF on the map side. */
		private void initMapSide(ObjectInspector[] parameters) throws HiveException {
			int length = parameters.length;
			outputOIs = new ObjectInspector[length];
			List<String> fieldNames = new ArrayList<String>(length);

			for (int i = 0; i < length; i++) {
				// 列名:col0、col1
				fieldNames.add("col" + i); // field names are not made available! :(
				// 获得输入参数类型所对应的标准OI 赋给 outputOIs数组
				outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(parameters[i]);
			}
			// 将outputOIs数组转成List
			List<ObjectInspector> fieldOIs = Arrays.asList(outputOIs);
			// 输入参数类型 赋给 inputOIs数组
			inputOIs = parameters;
			// 构造出struct类型的返回值
			// ObjectInspectorFactory is the primary way to create new ObjectInspector instances.
			structOI = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
		}

		/*
		 * Initialize the UDAF on the reduce side (or the map side in some  cases).
		 */
		private void initReduceSide(StructObjectInspector inputStructOI) throws HiveException {
			List<? extends StructField> fields = inputStructOI.getAllStructFieldRefs();
			int length = fields.size();
			inputOIs = new ObjectInspector[length];
			outputOIs = new ObjectInspector[length];
			for (int i = 0; i < length; i++) {
				StructField field = fields.get(i);
				// inputOIs获取hadoop类型
				inputOIs[i] = field.getFieldObjectInspector();
				// outputOIs获取hadoop类型所对应的standardOI
				outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(inputOIs[i]);
			}
			structOI = ObjectInspectorUtils.getStandardObjectInspector(inputStructOI);
		}

		@SuppressWarnings("deprecation")
		static class MaxAgg implements AggregationBuffer {
			// 用于保留比较列最大的行数据
			Object[] objects;
		}

		// 创建新的聚合计算的需要的内存,用来存储mapper,combiner,reducer运算过程中需要保存的行数据
		@SuppressWarnings("deprecation")
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MaxAgg result = new MaxAgg();
			return result;
		}

		// mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用
		@SuppressWarnings("deprecation")
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			MaxAgg maxagg = (MaxAgg) agg;
			maxagg.objects = null;
		}

		// map阶段调用，直接调用合并操作即可
		// iterate方法存在于MR的M阶段，用于处理每一条输入记录
		@SuppressWarnings("deprecation")
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			merge(agg, parameters);
		}

		// mapper结束要返回的结果，还有combiner结束返回的结果
		// terminatePartial方法在iterate处理完所有输入后调用，用于返回初步的聚合结果
		@SuppressWarnings("deprecation")
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return terminate(agg);
		}

		// combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果
		// merge方法存在于MR的R阶段（也同样存在于Combine阶段），用于最后的聚合
		@SuppressWarnings("deprecation")
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				MaxAgg maxagg = (MaxAgg) agg;
				List<Object> objects;
				if (partial instanceof Object[]) {
					objects = Arrays.asList((Object[]) partial);
				} else if (partial instanceof LazyBinaryStruct) {
					objects = ((LazyBinaryStruct) partial).getFieldsAsList();
				} else {
					throw new HiveException("Invalid type: " + partial.getClass().getName());
				}

				boolean isMax = false;
				// 第一行数据,object数组为空
				if (maxagg.objects == null) {
					isMax = true;
				} else {
					// 与最大的行数据进行比较
					// maxagg.objects[0]的类型是standardOI类型,而objects.get(0)为传入参数,是hadoop的数据类型,故应该分别使用outputOIs和inputOIs指定类型
					int cmp = ObjectInspectorUtils.compare(maxagg.objects[0], outputOIs[0], objects.get(0), inputOIs[0]);
					// 这样写报错：org.apache.hadoop.io.IntWritable cannot be cast to org.apache.hadoop.hive.serde2.lazy.LazyPrimitive
					// int cmp = ObjectInspectorUtils.compare(maxagg.objects[0], inputOIs[0], objects.get(0), inputOIs[0]);
					// 这样写报错：org.apache.hadoop.hive.serde2.lazy.LazyInteger cannot be cast to org.apache.hadoop.io.IntWritable
					// int cmp = ObjectInspectorUtils.compare(maxagg.objects[0], outputOIs[0], objects.get(0), outputOIs[0]);
					if (cmp < 0) {
						isMax = true;
					}
				}

				// 数据替换
				if (isMax) {
					int length = objects.size();
					maxagg.objects = new Object[length];
					for (int i = 0; i < length; i++) {
						maxagg.objects[i] = ObjectInspectorUtils.copyToStandardObject(objects.get(i), inputOIs[i]);
					}
				}
			}
		}

		// reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果
		// terminate方法在merge方法执行完毕之后调用，用于进行最后的处理，并返回最后结果
		@SuppressWarnings("deprecation")
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			MaxAgg maxagg = (MaxAgg) agg;
 			// return Arrays.asList(maxagg.objects); //作者的写法
			return maxagg.objects;  //我的理解
		}
	}
}

/**   
*	补充：  
*	public static enum Mode {  
*
*    	// 相当于map阶段，调用iterate()和terminatePartial() 
*    	PARTIAL1,  
*
*    	// 相当于combiner阶段，调用merge()和terminatePartial() 
*    	PARTIAL2,  
*
*    	// 相当于reduce阶段调用merge()和terminate() 
*    	FINAL,  
*
*    	// 相当于没有reduce阶段map，调用iterate()和terminate() 
*    	COMPLETE  
*	};
*/