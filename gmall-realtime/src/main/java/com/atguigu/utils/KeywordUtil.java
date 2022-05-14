package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {
        // 创建集合用于存放最终结果数据
        ArrayList<String> list = new ArrayList<>();

        StringReader stringReader = new StringReader(keyword);

        // 创建IK 分词对象
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, false);

        // 把
        Lexeme next = ikSegmenter.next();

        while (next != null) {
            String word = next.getLexemeText();
            list.add(word);
            next = ikSegmenter.next();
        }

        // 返回集合
        return list;
    }

    public static void main(String[] args) throws IOException {
        List<String> list = splitKeyWord("尚硅谷大数据项目之Flink实时数仓");
        System.out.println(list);
    }
}
