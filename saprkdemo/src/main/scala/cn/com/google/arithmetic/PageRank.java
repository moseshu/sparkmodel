package cn.com.google.arithmetic;

/**
 * Created by moses on 2017/11/30.
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


public class PageRank {

    private Map<String, Set<String>> outNet = null;
    private Map<String, Set<String>> inNet = null;
    private Set<String> users = null;
    private Set<String> seeds = new HashSet<String>();
    private Map<String, Double> rankScore = new HashMap<String, Double>();
    private double dampingFactor = 0;

    public PageRank(Map<String, Set<String>> outNet, Map<String, Set<String>> inNet, Set<String> users, double dampingFactor) {
        this.outNet = outNet;
        this.inNet = inNet;
        this.users = users;
        this.dampingFactor = dampingFactor;
    }

    public Map<String, Double> calcScore(int iterNum, boolean trustFlag) {

        int count = 0;
        double goodValue = 0.0f;

        while (count < iterNum) {

            Map<String, Double> lastRankScore = new HashMap<String, Double>(rankScore);

            for (String node : users) {

                double rank = 0.0;
                Set<String> inDegrees = inNet.get(node);

                if (inDegrees != null) {
                    for (String item : inDegrees) {
                        rank += lastRankScore.get(item) / outNet.get(item).size();
                    }
                }

                if (trustFlag) {
                    if (seeds.contains(node)) {
                        goodValue = 1.0f;
                    } else {
                        goodValue = 0.0f;
                    }
                    rankScore.replace(node, dampingFactor * rank + (1 - dampingFactor) * goodValue );
                } else {
                    rankScore.replace(node, dampingFactor * rank + (1 - dampingFactor) * 1.0 );
                }
            }

            double delta = 0.0;

            for (String node : users) {
                delta += Math.abs(rankScore.get(node) - lastRankScore.get(node));
            }

            count += 1;

            if (delta < 1e-8) {
                break;
            }
        }

        return rankScore;
    }

    public void initRank(Set<String> seeds, boolean goodFlag) {

        double goodValue = -1.0;

        if (goodFlag) {
            goodValue = 1.0;
        }

        if (seeds.size() == 0) {
            this.seeds.addAll(this.users);
        } else {
            this.seeds.addAll(seeds);
        }

        for (String user : users) {
            rankScore.put(user, 0.0);
            if (seeds.contains(user)) {
                rankScore.replace(user, goodValue );
            }
        }
    }

    public Map<String, Double> orderRank(final boolean reverse) {

        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();

        if (!rankScore.isEmpty() && rankScore != null) {

            List<Map.Entry<String, Double>> entryList = new ArrayList<Map.Entry<String, Double>>(rankScore.entrySet());

            Collections.sort(entryList,
                    new Comparator<Map.Entry<String, Double>>() {
                        public int compare(Entry<String, Double> entry1, Entry<String, Double> entry2) {
                            double v1 = 0;
                            double v2 = 0;
                            try {
                                v1 = entry1.getValue();
                                v2 = entry2.getValue();
                            } catch (NumberFormatException e) {
                                v1 = 0.0;
                                v2 = 0.0;
                            }

                            int flag = 0;
                            if (v2 > v1) {
                                flag = 1;
                            } else if (v2 < v1) {
                                flag = -1;
                            }

                            if (reverse)  {
                                flag *= -1;
                            }
                            return flag;
                        }
                    });

            Iterator<Map.Entry<String, Double>> iter = entryList.iterator();
            Map.Entry<String, Double> tmpEntry = null;
            while (iter.hasNext()) {
                tmpEntry = iter.next();
                sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
            }
        }

        return sortedMap;
    }

    public Map<String, Double> getRankScore() {
        return this.rankScore;
    }

}
