package com.neuedu.brazil;

import java.util.Scanner;

/**
 * 菜单与运行类
 *
 * @author zhang
 */
public class Starter {
    private static Scanner scanner = new Scanner(System.in);

    public static void showMenu() {
        System.out.println("**********************************");
        System.out.println("****天***气***查***询***系***统*****");
        System.out.println("**********************************");
        System.out.println("1、数据清洗与数据导入");
        System.out.println("2、查询指定日期的天气信息");
        System.out.println("3、查询每年的最高气温");
        System.out.println("4、查询每年的最低气温");
        System.out.println("5、查询每年的平均气温");
        System.out.println("6、查询每年的降雨天数");
        System.out.println("7、预测天气");
        System.out.println("0、退出");
        System.out.print("请选择（0--7）：");
    }

    public static void run() {
        boolean isExited = false;
        int choice = 0;
        do {
            showMenu();
            choice = scanner.nextInt();
            switch (choice) {
                case 1:
                    Step1.run();
                    break;
                case 2:
                    Step2.run();
                    break;
                case 3:
                    Step3.run();
                    break;
                case 4:
                    Step4.run();
                    break;
                case 5:
                    Step5.run();
                    break;
                case 6:
                    Step6.run();
                    break;
                case 7:
                    Step7.run();
                    break;
                case 0:
                    isExited = !isExited;
                    break;
                default:
                    System.out.println("输入错误，请重新输入~~~");
                    break;
            }
        } while (!isExited);
        System.out.println("感谢您的使用，退出系统~~~");
    }

    public static void main(String[] args) {
        run();
    }
}

