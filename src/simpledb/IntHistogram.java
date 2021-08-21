package simpledb;

import java.util.ArrayList;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private List<Bucket> bucketsList;

    private int buckets;
    private int min;
    private int max;
    private double width;
    private int tulCount;


    /**
     * [1,10]有1有10
     */
    class Bucket{
        private int left;
        private int right;
        private int count;

        public  Bucket(int left,int right){
            this.left = left;
            this.right = right;
        }
        public void add(){
            count++;
        }

        @Override
        public String toString() {
            return "Bucket{" +
                    "left=" + left +
                    ", right=" + right +
                    ", count=" + count +
                    '}';
        }
        public int getWid(){
            return right-left+1;
        }
    }

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.max = max;
        this.min = min;
        this.buckets = buckets;
        this.width = (max-min+1.0) / buckets;
        this.bucketsList = new ArrayList<>(buckets);
        for(int i=0;i<buckets;i++){
            int left = (int) Math.ceil (min + i * width);
            int right = (int)Math.ceil(left + width) - 1;
            if(right>max){
                right=max;
            }
            bucketsList.add(new Bucket(left,right));
        }
    }
    private int getIndex(int v){
        int index = (int)((v-min)/width);
        return index;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        bucketsList.get(getIndex(v)).add();
        tulCount++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = getIndex(v);
        switch(op){
            case EQUALS:
                if(index<0 || index>buckets){
                    return 0;
                }else{
                    return 1.0 * bucketsList.get(index).count / bucketsList.get(index).getWid() / tulCount;
                }
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN,v+1);
            case LESS_THAN:
                if(index < 0){
                    return 0;
                }else if ( index >= buckets){
                    return 1;
                }else{
                    int sum = 0;
                    for(int i = 0;i<index;i++){
                        sum += bucketsList.get(i).count;
                    }
                    sum += 1.0*bucketsList.get(index).count * (v - bucketsList.get(index).left) / bucketsList.get(index).getWid();
                    return sum / (double)tulCount;
                }
            case GREATER_THAN:
                if(index < 0){
                    return 1;
                }else if ( index >= buckets){
                    return 0;
                }else{
                    int sum = 0;
                    for(int i = index + 1;i<buckets;i++){
                        sum += bucketsList.get(i).count;
                    }
                    sum += 1.0 * bucketsList.get(index).count * (bucketsList.get(index).right - v) / bucketsList.get(index).getWid();
                    return sum / (double)tulCount;
                }
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN , v - 1);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS , v);
            default:
                throw new UnsupportedOperationException();
        }
    	// some code goes here
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        int cnt = 0;
        for(Bucket bucket : bucketsList){
            cnt += bucket.count;
        }
        // some code goes here
        return cnt / width;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Bucket bucket : bucketsList){
            sb.append(bucket);
        }
        // some code goes here
        return sb.toString();
    }
}
