package com.leandro.camara.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Classe de representação ao Spout do 'Yahoo Finance'.
 */
public class YahooFinanceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    /**
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = collector;
    }

    /**
     *
     */
    public void nextTuple() {
        try {
            String symbolCompany = "GOOG";
            StockQuote quote = this.getCompanyQuoteBySymbol(symbolCompany);

            this.collector.emit(new Values(
                    symbolCompany,
                    this.getCurrentDate(),
                    this.getPriceByQuote(quote),
                    this.getPreviousCloseByQuote(quote)
            ));
            Thread.sleep(1000);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company", "timestamp", "price", "previousClose"));
    }

    /**
     * @param symbol
     * @return
     * @throws IOException
     */
    private StockQuote getCompanyQuoteBySymbol(String symbol) throws IOException {
        return YahooFinance.get(symbol).getQuote();
    }

    /**
     * @param quote
     * @return
     */
    private Double getPriceByQuote(StockQuote quote) {
        return quote.getPrice().doubleValue();
    }

    /**
     * @param quote
     * @return
     */
    private Double getPreviousCloseByQuote(StockQuote quote) {
        return quote.getPreviousClose().doubleValue();
    }

    /**
     * @return
     */
    private String getCurrentDate() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        return simpleDateFormat.format(timestamp);
    }
}
