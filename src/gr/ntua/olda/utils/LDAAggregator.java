package gr.ntua.olda.utils;

import java.io.IOException;
import java.util.List;

import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vagueobjects.ir.lda.online.OnlineLDA;
import vagueobjects.ir.lda.online.Result;
import vagueobjects.ir.lda.tokens.Documents;

public class LDAAggregator implements ReducerAggregator<OnlineLDA> {

    private static final long serialVersionUID = 1L;

    private final static Logger logger = LoggerFactory.getLogger(LDAAggregator.class);

    private LDAVocabulary voc;

    // LDA algorithm parameters
    private static int D = LocalConfig.getInt("lda.algorithm.D ", 8524840);
    private static int K = LocalConfig.getInt("lda.algorithm.K ", 5);
    private static double tau = LocalConfig.getDouble("lda.algorithm.tau ", 1.0);
    private static double kappa =  LocalConfig.getDouble("lda.algorithm.kappa ", 0.8);
    private static double alpha = LocalConfig.getDouble("lda.algorithm.alpha ", 1.d/K);
    private static double eta = LocalConfig.getDouble("lda.algorithm.eta ", 1.d/K);

    @Override
    public OnlineLDA init() {
        OnlineLDA lda = null;

        try {
            logger.info("LDAAggregator: Initializing Values...");
            voc = new LDAVocabulary(LocalConfig.get("file.lda.dictionary.path"));
            lda = new OnlineLDA(voc.size(), K, D, alpha, eta, tau, kappa);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lda;
    }


    @Override
    public OnlineLDA reduce(OnlineLDA curr, TridentTuple tuple) {
        if (tuple.isEmpty())
            return curr;

        // Didn't figure out why init is not called but lost my patience so fuck*that*initialization :(
        if (voc == null) {
            try {
                logger.info("LDAAggregator: Initializing Values...");
                voc = new LDAVocabulary(LocalConfig.get("file.lda.dictionary.path"));
                OnlineLDA lda = new OnlineLDA(voc.size(), K, D, alpha, eta, tau, kappa);
                return process_current_batch(lda, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return process_current_batch(curr, tuple);
    }


    @SuppressWarnings("unchecked")
    private OnlineLDA process_current_batch(OnlineLDA curr, TridentTuple tuple) {
        List<String> docs = null;

        try {
            docs = (List<String>) tuple.getValue(0); // get docs here;
            Documents documents = new Documents(docs, voc);

            logger.info("LDAAggregator: Executing LDA algorithm");
            Result result = curr.workOn(documents);
            logger.info(result.toString());
        } catch (ArrayIndexOutOfBoundsException ex) {
            logger.error("LDAAggregator: Exception happened for docs size {} and exception error was {} ",
                    (docs != null ? docs.size() : 0), ex.getLocalizedMessage());
        }
        return curr;
    }
}
