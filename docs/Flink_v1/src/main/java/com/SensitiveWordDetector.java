package com;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * 敏感词检测器 - IK分词修复版
 */
public class SensitiveWordDetector {

    private static final String BASE_PATH = "D:\\soft2\\stream-bda-prod\\maven_v1\\stream-core\\src\\main\\resources\\";
    private static final Map<String, String> SENSITIVE_WORDS = new HashMap<String, String>();
    private static final Map<String, Integer> BAN_DAYS = new HashMap<String, Integer>();

    static {
        // 初始化封禁配置
        BAN_DAYS.put("P0", 365);
        BAN_DAYS.put("P1", 60);
        BAN_DAYS.put("P2", 0);

        loadWordLibrary();
    }

    /**
     * 加载词库
     */
    private static void loadWordLibrary() {
        try {
            loadWordsFromFile("p0_words.txt", "P0");
            loadWordsFromFile("p1_words.txt", "P1");
            loadWordsFromFile("p2_words.txt", "P2");

            System.out.println("敏感词库加载完成 - P0:" + getWordCount("P0") +
                    ", P1:" + getWordCount("P1") +
                    ", P2:" + getWordCount("P2"));

            // 打印P0级别的关键词用于调试
            System.out.println("P0关键词示例: " + getSampleWords("P0", 10));
        } catch (Exception e) {
            System.err.println("加载敏感词库失败: " + e.getMessage());
        }
    }

    private static long getWordCount(String level) {
        long count = 0;
        for (String value : SENSITIVE_WORDS.values()) {
            if (level.equals(value)) {
                count++;
            }
        }
        return count;
    }

    private static List<String> getSampleWords(String level, int max) {
        List<String> samples = new ArrayList<String>();
        for (Map.Entry<String, String> entry : SENSITIVE_WORDS.entrySet()) {
            if (level.equals(entry.getValue()) && samples.size() < max) {
                samples.add(entry.getKey());
            }
        }
        return samples;
    }

    /**
     * 从文件加载词库
     */
    private static void loadWordsFromFile(String fileName, String level) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(BASE_PATH + fileName));
            int count = 0;
            for (String line : lines) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    SENSITIVE_WORDS.put(line, level);
                    count++;
                }
            }
            System.out.println("加载 " + level + " 词库: " + count + " 个词");
        } catch (Exception e) {
            System.err.println("加载词库文件失败 " + fileName + ": " + e.getMessage());
        }
    }

    /**
     * 使用IK分词进行文本分词
     */
    private static List<String> segmentText(String text) {
        List<String> segments = new ArrayList<String>();
        if (text == null || text.trim().isEmpty()) {
            return segments;
        }

        try {
            StringReader reader = new StringReader(text);
            IKSegmenter segmenter = new IKSegmenter(reader, true); // 智能分词

            Lexeme lexeme;
            while ((lexeme = segmenter.next()) != null) {
                String word = lexeme.getLexemeText();
                if (word != null) {
                    segments.add(word);
                }
            }
        } catch (Exception e) {
            System.err.println("IK分词异常: " + e.getMessage());
        }

        return segments;
    }

    /**
     * 检测敏感词 - 修复版：结合原始文本匹配和分词匹配
     */
    public static SensitiveResult detect(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new SensitiveResult(false, "CLEAN", "", new ArrayList<String>());
        }

        List<String> foundWords = new ArrayList<String>();
        String maxLevel = "CLEAN";
        String firstTriggeredWord = "";

        System.out.println("🔍 开始检测文本: " + text);

        // 方法1: 直接在整个文本中匹配敏感词（确保能匹配到完整词汇）
        for (Map.Entry<String, String> entry : SENSITIVE_WORDS.entrySet()) {
            String word = entry.getKey();
            String level = entry.getValue();

            if (text.contains(word)) {
                System.out.println("✅ 直接匹配到敏感词: " + word + " -> " + level);

                if (!foundWords.contains(word)) {
                    foundWords.add(word);
                }

                if (firstTriggeredWord.isEmpty()) {
                    firstTriggeredWord = word;
                }

                // 更新最高级别
                if (getLevelWeight(level) > getLevelWeight(maxLevel)) {
                    maxLevel = level;
                }
            }
        }

        // 方法2: 使用IK分词进行细粒度匹配（用于匹配分词后的词汇）
        if (foundWords.isEmpty()) {
            List<String> segments = segmentText(text);
            System.out.println("分词结果: " + segments);

            for (String segment : segments) {
                for (Map.Entry<String, String> entry : SENSITIVE_WORDS.entrySet()) {
                    String word = entry.getKey();
                    String level = entry.getValue();

                    // 分词匹配敏感词
                    if (segment.equals(word) || segment.contains(word)) {
                        System.out.println("✅ 分词匹配到敏感词: " + word + " -> " + level + " (分词: " + segment + ")");

                        if (!foundWords.contains(word)) {
                            foundWords.add(word);
                        }

                        if (firstTriggeredWord.isEmpty()) {
                            firstTriggeredWord = word;
                        }

                        if (getLevelWeight(level) > getLevelWeight(maxLevel)) {
                            maxLevel = level;
                        }
                    }
                }
            }
        }

        // 按优先级重新排序找到的敏感词，确保最高级别的词排在前面
        if (!foundWords.isEmpty()) {
            Collections.sort(foundWords, new Comparator<String>() {
                @Override
                public int compare(String word1, String word2) {
                    String level1 = SENSITIVE_WORDS.get(word1);
                    String level2 = SENSITIVE_WORDS.get(word2);
                    return Integer.compare(getLevelWeight(level2), getLevelWeight(level1));
                }
            });

            // 更新第一个触发的关键词为最高级别的词
            firstTriggeredWord = foundWords.get(0);
        }

        boolean isSensitive = !foundWords.isEmpty();

        System.out.println("📊 检测结果: 敏感=" + isSensitive +
                ", 级别=" + maxLevel +
                ", 触发关键词=" + firstTriggeredWord +
                ", 所有检测到的词=" + foundWords +
                ", 封禁天数=" + getBanDays(maxLevel));

        return new SensitiveResult(isSensitive, maxLevel, firstTriggeredWord, foundWords);
    }

    /**
     * 获取级别权重
     */
    private static int getLevelWeight(String level) {
        if ("P0".equals(level)) {
            return 3;
        } else if ("P1".equals(level)) {
            return 2;
        } else if ("P2".equals(level)) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 获取封禁天数
     */
    public static int getBanDays(String level) {
        Integer days = BAN_DAYS.get(level);
        return days != null ? days : 0;
    }

    /**
     * 敏感检测结果
     */
    public static class SensitiveResult {
        public boolean isSensitive;
        public String level;
        public String triggeredKeyword;
        public List<String> foundWords;

        public SensitiveResult(boolean isSensitive, String level, String triggeredKeyword, List<String> foundWords) {
            this.isSensitive = isSensitive;
            this.level = level;
            this.triggeredKeyword = triggeredKeyword;
            this.foundWords = foundWords;
        }

        public int getBanDays() {
            return SensitiveWordDetector.getBanDays(level);
        }
    }
}