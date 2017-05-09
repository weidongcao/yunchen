package com.rainsoft.j2se;

import com.rainsoft.util.java.NumberUtils;
import net.sf.json.JSONArray;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * {imsi_code},{phone_num},{area_name},{area_code},{capture_time},{service_code},{service_name},{phone_type},{area_brief},{people_brief},{people_rank},{people_type},{doubtful_period},{doubtful_days},{doubtful_times}
 */
public class EmphasisCreateData {
    public static JSONArray serviceJsonArray;
    public static String isarea = "[56,55,54,53,52,51,50,37,49,48,47,46,45,44,43,42,41,40,36,39]";
    public static JSONArray isareaJSON = JSONArray.fromObject(isarea);
    public static JSONArray areaJsonArray;
    public static String ispeople = "[63,62,20,19,60,59,58,57,56,26,55,54,53,52,51,50,49,48,47,46]";
    public static JSONArray ispeopleJSON = JSONArray.fromObject(ispeople);
    public static JSONArray peopleJsonArray;
    public static String[] doublefulArray = new String[3];


    public static String imsi_phone;
    public static String[] imsi_phoneArray;
    public static String separator = ",";
    public static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static Random random = new Random();

    static {
        try {
            serviceJsonArray = JSONArray.fromObject(FileUtils.readFileToString(new File("D:\\0WorkSpace\\Develop\\data\\community.txt")));
            areaJsonArray = JSONArray.fromObject(JSONArray.fromObject(FileUtils.readFileToString(new File("D:\\0WorkSpace\\Develop\\data\\area.txt"))));
            peopleJsonArray = JSONArray.fromObject(JSONArray.fromObject(FileUtils.readFileToString(new File("D:\\0WorkSpace\\Develop\\data\\crime.txt"))));
            imsi_phone = FileUtils.readFileToString(new File("D:\\0WorkSpace\\Develop\\data\\aaa"));
            doublefulArray[0] = "3,2,5";
            doublefulArray[1] = "5,3,7";
            doublefulArray[2] = "7,4,10";
            imsi_phoneArray = imsi_phone.split("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        aaa();
    }

    public static void aaa() throws IOException, ParseException {
        Calendar calendar = Calendar.getInstance();
        Date curDate = dateFormat.parse("2017-04-08 03:04:00");
        calendar.setTime(curDate);

        for (int j = 0; j < 720; j++) {
            createEmphasisData(calendar);
            calendar.add(Calendar.HOUR, 1);

        }
    }

    public static void createEmphasisData(Calendar calendar) throws IOException {
        String month = NumberUtils.getFormatInt(2, 2, calendar.get(Calendar.MONTH) + 1);
        String day = NumberUtils.getFormatInt(2, 2, calendar.get(Calendar.DATE));
        String hour = NumberUtils.getFormatInt(2, 2, calendar.get(Calendar.HOUR_OF_DAY));
        String minute = NumberUtils.getFormatInt(2, 2, calendar.get(Calendar.MINUTE));
        File temp_emphasis = new File("D:\\0WorkSpace\\Develop\\data\\temp_emphasis\\temp_emphasis_data_" + month + day + hour + minute + ".txt");

        if (temp_emphasis.exists()) {
            temp_emphasis.delete();
        }
        temp_emphasis.createNewFile();

        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<>();
        // {imsi_code},{phone_num},{area_name},{area_code},{capture_time},{service_code},{service_name},{phone_type},{area_brief},{people_brief},{people_rank},{people_type},{doubtful_period},{doubtful_days},{doubtful_times}
        for (int i = 0; i < 20; i++) {
            int index = random.nextInt(20);
            String line = imsi_phoneArray[index];
            String[] arr = line.split(",");
            String aaa = line.replaceAll("[(移动)(联通)(电信)]", "");
            //{imsi_code},{phone_num},{area_name},{area_code},
            sb.append(aaa);
            //{capture_time},
            sb.append(dateFormat.format(calendar.getTime()) + ",");

            int serIndex;
            while (true) {
                serIndex = random.nextInt(3);
                if (list.contains(965310 + serIndex + "->" + aaa) == false) {
                    list.add(965311 + serIndex + "->" + aaa);
                    //{service_code},
                    sb.append(965311 + serIndex);
                    sb.append(",");
                    //{service_name},
                    sb.append(serviceJsonArray.optString(serIndex) + ",");
                    break;
                }
            }
            //{phone_type},
            sb.append(arr[4] + ",");

            String phone7 = arr[1];

            int bbb = Integer.valueOf(phone7.substring(5, 7));
            //{area_brief},
            if (isareaJSON.contains(bbb)) {
                sb.append(areaJsonArray.optString(bbb / 5));
                sb.append(",");
            } else {
                sb.append("null,");
            }

            //{people_brief},{people_rank},{people_type},
            if (ispeopleJSON.contains(bbb)) {
                sb.append(peopleJsonArray.optString(bbb / 10));
                sb.append(",");
                sb.append(bbb / 10);
                sb.append(",");
                sb.append("1,");
            } else {
                sb.append("null,null,null,");
            }

            /**
             * "3,2,5"  :   "高嘉花园"
             * "5,3,7"  :   "文星花园"
             * "7,4,10" :   "桂庙新村"
             */
            //{doubtful_period},{doubtful_days},{doubtful_times}
            if (serIndex == 0) {
                sb.append(doublefulArray[0]);
            } else if (serIndex == 1) {
                sb.append(doublefulArray[1]);
            } else {
                sb.append(doublefulArray[2]);
            }
            sb.append("\n");
        }
        list.clear();

        FileUtils.writeStringToFile(temp_emphasis, sb.toString());
    }
}
