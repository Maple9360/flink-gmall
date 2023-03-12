package com.maple.utils;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.WordDictionary;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class KeywordUtil {


    public static List<String> splitKeyword(String keyword) throws IOException {

        // 创建集合用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();

        // 创建IK分词
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader,true);

        // 循环IK分词对象
        Lexeme next = ikSegmenter.next();
        while (next != null){
            String word = next.getLexemeText();
            list.add(word);
            next = ikSegmenter.next();
        }

        // 返回最终集合
        return list;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyword("今天学习大数据实时数仓项目Flink"));


        System.out.println(jiebaSplit("jieba>>>>" + "今天学习大数据实时数仓项目Flink"));

        System.out.println(jiebaCustomSplit(" jiebaCustom>>>>>" + "今天学习大数据实时数仓项目Flink"));



    }



    public static List<String> jiebaSplit(String word) {


        JiebaSegmenter segmenter = new JiebaSegmenter();
        return segmenter.sentenceProcess(word);

    }

    public static List<String> jiebaCustomSplit(String word) {

        // 加载自定义的词典
        //Path path = FileSystems.getDefault().getPath("D:\\ChromeCoreDownloads\\q\\gmall-flink\\data\\1.txt");

        // 词典路径为Resource/dicts/jieba.dict
        Path path = Paths.get("D:\\ChromeCoreDownloads\\q\\gmall-flink\\data\\1.txt");
        WordDictionary.getInstance().loadUserDict(path);

        JiebaSegmenter jiebaSegmenter = new JiebaSegmenter();
        return jiebaSegmenter.sentenceProcess(word);

    }


}
