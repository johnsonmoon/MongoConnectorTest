package xuyihao.mongo.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by xuyh at 2017/4/16 10:44.
 */
public class CommonUtils {
    private static BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    /**
     * 控制台输入一行
     *
     * @return
     */
    public static String inputLine(){
        String message = "";
        try {
            message = reader.readLine();
        }catch (Exception e){
            e.printStackTrace();
        }
        return message;
    }

    /**
     * 控制台输出一行信息
     *
     * @param lineMessage
     */
    public static void outputLine(String lineMessage){
        System.out.println(lineMessage);
    }
}
