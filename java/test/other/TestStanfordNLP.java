package other;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class TestStanfordNLP {

	@Test
	public void testSentenceSplit() {
		Properties props = new Properties();		
		props.put("annotators", "tokenize, ssplit");
		props.put("ssplit.boundariesToDiscard", "[[,]]");
		Annotation text = new Annotation("[[Kosgi. Santosh]] sent an email to Stanford University. He didn't get a reply\nShit always happens.");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);		

		// run all Annotators on this text
		pipeline.annotate(text);

		// get all annotated sentences
		List<CoreMap> sentences = 
				text.get(SentencesAnnotation.class);

		for (CoreMap sen : sentences) {
			System.out.println(sen.get(TextAnnotation.class));
		}
		
	}
}
