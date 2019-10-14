package com.leandro.camara.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Classe de representação ao Bolt do 'Yahoo Finance'.
 */
public class YahooFinanceBolt extends BaseBasicBolt {

    /**
     * Caso necessário, prepara informações relevantes e compartilhadas entre os 'Bolts'.
     *
     * @param topoConf
     * @param context
     */
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {}

    /**
     * Método responsável pela execução do 'Bolt'.
     *
     * @param tuple
     * @param basicOutputCollector
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Double price = tuple.getDoubleByField("price");
        String company = tuple.getStringByField("company");
        String currentDate = tuple.getStringByField("currentDate");
        Double previousClose = tuple.getDoubleByField("previousClose");

        Boolean gain = this.isPriceWithGain(price, previousClose);

        System.out.println(gain);
        System.out.println(price);
        System.out.println(company);
        System.out.println(currentDate);
        System.out.println(previousClose);

        basicOutputCollector.emit(new Values(
                company,
                currentDate,
                price,
                previousClose
        ));
    }

    /**
     * Define os campos das tuplas (resultantes).
     *
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

    /**
     * Método executado após a finalização da execução do 'Bolt'.
     */
    @Override
    public void cleanup() {}

    private Boolean isPriceWithGain(Double price, Double previousClose) {
        boolean gain = true;

        if (price > previousClose) {
            System.out.println("Ocorreu ganho!");
        } else {
            gain = false;
            System.out.println("Ocorreu perda ou manteve o valor da ação!");
        }
        return gain;
    }
}
