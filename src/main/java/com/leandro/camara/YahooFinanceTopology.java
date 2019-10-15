package com.leandro.camara;

import com.leandro.camara.bolt.YahooFinanceBolt;
import com.leandro.camara.spout.YahooFinanceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Classe responsável pela inicialização do Topologia 'Yahoo Finance'.
 *
 * @author Leandro Câmara
 */
public class YahooFinanceTopology {

    /**
     * Método responsável pela execução da Topologia 'Yahoo Finance'.
     *
     * @param args Parâmetros
     */
    public static void main(String[] args) throws Exception {
        LocalCluster cluster = new LocalCluster();
        try {
            TopologyBuilder builder = getTopologyBuilder();
            cluster.submitTopology("YahooFinanceTopology", getConfig(), builder.createTopology());
            Thread.sleep(50000);
        } catch (Exception e) {
            cluster.shutdown();
        }
    }

    /**
     * Retorna o Topologia com o mapeamento dos 'Spouts' e 'Bolts'.
     *
     * @return
     */
    private static TopologyBuilder getTopologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("yahooSpout", new YahooFinanceSpout());
        builder.setBolt("yahooBolt", new YahooFinanceBolt()).shuffleGrouping("yahooSpout");

        return builder;
    }

    /**
     * Retorna as configurações a serem utilizadas na topologia.
     *
     * @return
     */
    private static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }
}
