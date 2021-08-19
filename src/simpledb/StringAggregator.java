package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private int result;
    private Map<Field,Integer> map ;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if(what!=Op.COUNT){
            throw new IllegalArgumentException();
        }else{
            this.what = what;
        }
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupFiled;
        if (gbfield == NO_GROUPING){
            result++;
        }else{
            groupFiled = tup.getField(gbfield);
            if(!map.containsKey(groupFiled)){
                map.put(groupFiled,1);
            }else{
                map.put(groupFiled,map.get(groupFiled)+1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if(gbfield!=NO_GROUPING){
            TupleDesc tupleDesc =new TupleDesc(
                    new Type[]{gbfieldtype,Type.INT_TYPE}
            );
            List<Tuple> list = new ArrayList<>();
            for(Field field : map.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0,field);
                tuple.setField(1,new IntField(map.get(field)));
                list.add(tuple);
                System.out.println(tuple);
            }
            return new TupleIterator(tupleDesc,list);
        }else{
            TupleDesc tupleDesc = new TupleDesc(
                    new Type[]{Type.INT_TYPE}
            );
            List<Tuple> list = new ArrayList<>();
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(result));
            list.add(tuple);
            return new TupleIterator(tupleDesc,list);
        }

    }

}
