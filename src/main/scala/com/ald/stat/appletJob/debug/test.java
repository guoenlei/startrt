package com.ald.stat.appletJob.debug;

class test {
    public static void main(String[] args) {
        String input = "dsfadsf567781234567812345678afddsafads123f23fs212";
        String output = GetMaxNumber(input);
        System.out.println(output);
    }
    public static String GetMaxNumber(String inputStr){
        if(inputStr == null && inputStr.length() >= 10000 && inputStr.length() <= 0) {
            return "input str illegal";
        }

        //input str 2 char[]
        char[] strArray = inputStr.toCharArray();
        //start index
        int startIndex = 0;
        //当前处理的字符长度
        int tempCount = 0;
        //数字的最长长度
        int maxLen = 0;
        //数组的总长度
        int arrLength = strArray.length;
        int pos = 0;
        while (startIndex < arrLength)
        {
            //循环中的临时最大长度
            int tempMax = 0;
            while (tempCount + startIndex < arrLength)
            {
                //开始处理的字符
                char c = strArray[tempCount + startIndex];
                if ((c>='0')&&(c<='9'))
                {
                    //如果是数字
                    tempMax++;
                    if (tempMax > maxLen)
                    {
                        maxLen = tempMax;
                        pos = startIndex;
                    }
                }
                else
                {
                    //不是数字
                    tempMax = 0;
                    startIndex++;
                    break;
                }
                tempCount++;
            }
            if (startIndex + tempCount == arrLength)
            {
                break;
            }
            tempCount = 0;
        }
        String s = inputStr.substring(pos,pos+maxLen);
        return s;
    }
}
