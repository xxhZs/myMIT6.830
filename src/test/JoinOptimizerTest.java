package test;

import simpledb.JoinOptimizer;
import simpledb.LogicalJoinNode;
import simpledb.LogicalPlan;

import java.util.Set;
import java.util.Vector;

/**
 * @author yourname <xuxinhao@kuaishou.com>
 * Created on 2021-08-22
 */
public class JoinOptimizerTest {
    public static void main(String[] args) {
        Vector<Integer> v =new Vector<>();
        v.add(1);
        v.add(2);
        v.add(3);
        v.add(4);
        v.add(5);
        JoinOptimizer joinOptimizer = new JoinOptimizer(new LogicalPlan(),new Vector<LogicalJoinNode>());
        Set<Set<Integer>> sets = joinOptimizer.enumerateSubsets(v, 2);
        for(Set set:sets){
            System.out.println(set);
        }
    }
}
