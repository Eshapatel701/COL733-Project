package benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

public class GraphUtils {

    public static void generateIntegerGraph(String title, Map<Integer, Long> data, String xAxisLabel, String yAxisLabel, String fileName) {
        List<Integer> xData = new ArrayList<>(data.keySet());
        List<Long> yData = new ArrayList<>(data.values());

        XYChart chart = new XYChartBuilder().width(800).height(600).title(title).xAxisTitle(xAxisLabel).yAxisTitle(yAxisLabel).build();
        chart.addSeries(title, xData, yData);

        try {
            BitmapEncoder.saveBitmap(chart, fileName, BitmapEncoder.BitmapFormat.PNG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void generateLongGraph(String title, Map<Long, Long> data, String xAxisLabel, String yAxisLabel, String fileName) {
        List<Long> xData = new ArrayList<>(data.keySet());
        List<Long> yData = new ArrayList<>(data.values());

        XYChart chart = new XYChartBuilder().width(800).height(600).title(title).xAxisTitle(xAxisLabel).yAxisTitle(yAxisLabel).build();
        chart.addSeries(title, xData, yData);

        try {
            BitmapEncoder.saveBitmap(chart, fileName, BitmapEncoder.BitmapFormat.PNG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void generateBarGraph(String title, Map<String, Long> data, String xAxisLabel, String yAxisLabel, String fileName) {
        List<String> xData = new ArrayList<>(data.keySet());
        List<Long> yData = new ArrayList<>(data.values());

        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(title).xAxisTitle(xAxisLabel).yAxisTitle(yAxisLabel).build();
        chart.addSeries(title, xData, yData);

        try {
            BitmapEncoder.saveBitmap(chart, fileName, BitmapEncoder.BitmapFormat.PNG);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}