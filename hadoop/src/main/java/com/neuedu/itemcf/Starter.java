package com.neuedu.itemcf;

import java.util.HashMap;
import java.util.Map;


public class Starter {
    /**
     * map集合存储评分的依据
     */
    public static Map<String, Integer> R = new HashMap<String, Integer>();

    static {
        // 浏览商品
        R.put("click", 1);
        // 收藏商品
        R.put("collect", 2);
        // 加入购物车
        R.put("cart", 3);
        // 已付款
        R.put("pay", 4);
    }

    public static void main(String[] args) {
        // 串联N个MR任务，完成推荐商品
        Map<String, String> paths = new HashMap<String, String>();
        paths.put("step1_input", "/orderitems");
        paths.put("step1_output", "/cf1");
        paths.put("step2_input", "/cf1");
        paths.put("step2_output", "/cf2");
        paths.put("step3_input", "/cf2");
        paths.put("step3_output", "/cf3");
        paths.put("step4_input1", "/cf2");
        paths.put("step4_input2", "/cf3");
        paths.put("step4_output", "/cf4");
        paths.put("step5_input", "/cf4");
        paths.put("step5_output", "/cf5");
        paths.put("step6_input", "/cf5");
        paths.put("step6_output", "/cf6");

        //执行第1个MR任务:
        Step1.run(paths);
        //执行第2个MR任务:
        Step2.run(paths);
        //执行第3个MR任务:
        Step3.run(paths);
        //执行第4个MR任务:
        Step4.run(paths);
        //执行第5个MR任务:
        Step5.run(paths);
        //执行第6个MR任务:
        Step6.run(paths);
    }

}
