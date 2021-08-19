package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private double result;
    private Map<Field, Double> valMap;
    private Map<Field, Integer> numMap;
    private int cnt;


    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;
        this.gbfieldtype=gbfieldtype;
        this.numMap = new HashMap<>();
        this.valMap = new HashMap<>();
        this.result = Double.MIN_VALUE;
        this.cnt = 0;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField intField = (IntField) tup.getField(afield);
        double value = intField.getValue();
        //System.out.println(value);
        if (this.gbfield == NO_GROUPING) {
            switch (what) {
                case MIN:
                    if (result == Double.MIN_VALUE) {
                        result = value;
                    } else {
                        result = Math.min(result, value);
                    }
                    break;
                case MAX:
                    if (result == Double.MIN_VALUE) {
                        result = value;
                    } else {
                        result = Math.max(result, value);
                    }
                    break;
                case COUNT:
                    if (result == Double.MIN_VALUE) {
                        result = 1;
                    } else {
                        result++;
                    }
                    break;
                case SUM:
                    if (result == Double.MIN_VALUE) {
                        result = value;
                    } else {
                        result += value;
                    }
                    break;
                case AVG:
                    if (result == Double.MAX_VALUE) {
                        result = value;
                        cnt++;
                    } else {
                        cnt++;
                        result = (result * (cnt - 1) + value) / cnt;
                    }
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        } else {
            Field groupFiled = tup.getField(gbfield);
            switch (what) {
                case MIN:
                    if (!valMap.containsKey(groupFiled)) {
                        valMap.put(groupFiled, value);
                    } else {
                        valMap.put(groupFiled, Math.min(valMap.get(groupFiled), value));
                    }
                    break;
                case MAX:
                    if (!valMap.containsKey(groupFiled)) {
                        valMap.put(groupFiled, value);
                    } else {
                        valMap.put(groupFiled, Math.max(valMap.get(groupFiled), value));
                    }
                    break;
                case COUNT:
                    if (!valMap.containsKey(groupFiled)) {
                        valMap.put(groupFiled, 1D);
                    } else {
                        valMap.put(groupFiled, valMap.get(groupFiled) + 1);
                    }
                    break;
                case SUM:
                    if (!valMap.containsKey(groupFiled)) {
                        valMap.put(groupFiled, value);
                    } else {
                        valMap.put(groupFiled, valMap.get(groupFiled) + value);
                    }
                    break;
                case AVG:
                    if (!valMap.containsKey(groupFiled)) {
                        valMap.put(groupFiled, value);
                        numMap.put(groupFiled, 1);
                    } else {
                        numMap.put(groupFiled, numMap.get(groupFiled) + 1);
                        valMap.put(groupFiled, (valMap.get(groupFiled) * (numMap.get(groupFiled) - 1) + value) / numMap.get(groupFiled));
                    }
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

        /**
         * Create a OpIterator over group aggregate results.
         *
         * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
         *         if using group, or a single (aggregateVal) if no grouping. The
         *         aggregateVal is determined by the type of aggregate specified in
         *         the constructor.
         */
        public OpIterator iterator () {
            // some code goes here
            if(gbfield==NO_GROUPING){
                TupleDesc tupleDesc = new TupleDesc(
                        new Type[]{Type.INT_TYPE}
                );
                List<Tuple> list = new ArrayList<>();
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0,new IntField((int)result));
                list.add(tuple);
                return new TupleIterator(tupleDesc,list);
            }else{
                TupleDesc tupleDesc1 = new TupleDesc(
                        new Type[]{gbfieldtype,Type.INT_TYPE}
                );
                List<Tuple> list = new ArrayList<>();
                for(Field group : valMap.keySet()){
                    Tuple tuple = new Tuple(tupleDesc1);
                    tuple.setField(0,group);
                    tuple.setField(1,new IntField((int)(double)valMap.get(group)));

                    list.add(tuple);
                }
                return new TupleIterator(tupleDesc1,list);
            }
            //throw new UnsupportedOperationException("please implement me for lab2");
        }


}
