package com.leandro.camara;

import com.leandro.camara.bolt.YahooFinanceBolt;
import com.leandro.camara.spout.YahooFinanceSpout;
import org.apache.storm.Config;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Classe responsável pela inicialização do Topologia 'Yahoo Finance'.
 *
 * @author Leandro Câmara
 */
public class YahooFinanceTopology extends ConfigurableTopology {

    /**
     * Inicializa a topologia.
     *
     * @param args Parâmetros
     */
    public static void main(String[] args) {
        ConfigurableTopology.start(new YahooFinanceTopology(), args);
    }

    /**
     * Método responsável pela execução da Topologia 'Yahoo Finance'.
     *
     * @param strings
     * @return
     * @throws Exception
     */
    protected int run(String[] strings) throws Exception {
        return submit("YahooFinanceTopology", this.getConfig(), this.getTopologyBuilder());
    }

    /**
     * Retorna o Topologia com o mapeamento dos 'Spouts' e 'Bolts'.
     *
     * @return
     */
    private TopologyBuilder getTopologyBuilder() {
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
    private Config getConfig() {
        Config config = new Config();
        config.setDebug(true);

        return config;
    }
}
