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
import java.util.Set;
import java.util.Map.Entry;

public class TrustRank {

    private Set<String> goodSeeds = null;
    private Set<String> badSeeds = null;
    private double dampingFactor = 0.5f;
    private Map<String, Set<String>> outNet = null;
    private Map<String, Set<String>> inNet = null;
    private Set<String> users = null;
    private Map<String, Double> badScore = new HashMap<>();
    private Map<String, Double> goodScore = new HashMap<>();

    public TrustRank(Map<String, Set<String>> outNet, Map<String, Set<String>> inNet, Set<String> users) {
        this.outNet = outNet;
        this.inNet = inNet;
        this.users = users;
    }

    public void setBadSeeds(Set<String> badSeeds) {
        this.badSeeds = new HashSet<String>(badSeeds);
    }

    public void setGoodSeeds(Set<String> goodSeeds) {
        this.goodSeeds = new HashSet<String>(goodSeeds);
    }

    public void setDampingFactor(double dampingFactor) {
        this.dampingFactor = dampingFactor;
    }

    public Map<String, Double> orderRank(Map<String, Double> rankScore, final boolean reverse) {

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

                            if (reverse) {
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


    public Map<String, Double> run() {

        Map<String, Double> result = new HashMap<String, Double>();
        Map<String, Double> bres = null;
        Map<String, Double> gres = null;

        PageRank goodRank;
        PageRank badRank = null;

        if (null != goodSeeds) {
            goodRank = new PageRank(outNet, inNet, users, dampingFactor);
            goodRank.initRank(goodSeeds, true);
            gres = goodRank.calcScore(100, true);
            goodScore = gres;
            System.out.println("gres size: " + gres.size());
        }

        if (null != badSeeds) {
            badRank = new PageRank(inNet, outNet, users, dampingFactor);
            badRank.initRank(badSeeds, true);
            bres = badRank.calcScore(100, true);
            badScore = bres;
            System.out.println("bres size: " + bres.size());
        }

        if (null != bres && null != gres) {
            for (Map.Entry<String, Double> item : bres.entrySet()) {
                String key = item.getKey();
                result.put(key, bres.get(key) - gres.get(key));
            }
        } else if (null != bres) {
            result = bres;
        } else if (null != gres) {
            result = gres;
        }

        result = orderRank(result, true);
        System.out.println("result size: " + result.size());
        return result;
//		Map<String, Boolean> retValue = new HashMap<String, Boolean>();
//
//		int retNum = (int) (result.size() * threshold);
//		int count = 0;
//
//		for (Map.Entry<String, Double> item : result.entrySet()) {
//			if (count < retNum) {
//				retValue.put(item.getKey(), true);
//			} else {
//				retValue.put(item.getKey(), false);
//			}
//
//			count += 1;
//		}
//
//		return retValue;
    }

    public Map<String, Double> getGoodScore() {
        return goodScore;
    }
    public Map<String,Double> getBadScore(){
        return badScore;
    }
}