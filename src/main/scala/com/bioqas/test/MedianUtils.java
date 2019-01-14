package com.bioqas.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by chailei on 18/6/8.
 */
public class MedianUtils {


        /**
         * @param nums: A list of integers.
         * @return: An integer denotes the middle number of the array.
         */
        public static int median(int []nums){
            if(nums.length==0)
                return 0;
            int start=0;
            int end=nums.length-1;
            int index=partition(nums, start, end);
            if(nums.length%2==0){
                while(index!=nums.length/2-1){
                    if(index>nums.length/2-1){
                        index=partition(nums, start, index-1);
                    }else{
                        index=partition(nums, index+1, end);
                    }
                }
            }else{
                while(index!=nums.length/2){
                    if(index>nums.length/2){
                        index=partition(nums, start, index-1);
                    }else{
                        index=partition(nums, index+1, end);
                    }
                }
            }
            return nums[index];
        }
        private static int partition(int nums[], int start, int end){
            int left=start;
            int right=end;
            int pivot=nums[left];
            while(left<right){
                while(left<right&&nums[right]>=pivot){
                    right--;
                }
                if(left<right){
                    nums[left]=nums[right];
                    left++;
                }
                while(left<right&&nums[left]<=pivot){
                    left++;
                }
                if(left<right){
                    nums[right]=nums[left];
                    right--;
                }
            }
            nums[left]=pivot;
            return left;
        }


    public static double middle(List<Double> data){
        double mid = 0;
        Collections.sort(data);
        int len = data.size();
        System.out.println(data.toString());
        if(len%2==0) mid = (data.get(len/2-1)+data.get(len/2))/2; else mid = data.get(len/2);
        System.out.println("数组data的中位数为："+mid);
        return mid;
    }

    public static void main(String[] args) {


        List<Double> data = new ArrayList<Double>();
        data.add(1d);


        System.out.println(middle(data));
    }


}
