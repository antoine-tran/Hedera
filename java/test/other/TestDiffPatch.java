package other;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


import org.junit.Test;

import difflib.Delta;
import difflib.DiffAlgorithm;
import difflib.DiffUtils;

public class TestDiffPatch {

	
	@Test
	public void testNewAlgo() {
		List<String> original = new ArrayList<>();
		//original.add("The quick brown fox jumps over the lazy dog");
		//original.add("This sentence stays the same");
		//original.add("But this will be removed.");
		//original.add("This will be the same.");
		
        List<String> revised  = new ArrayList<>();
        revised.add("And this has been added.");
        revised.add("That quick brown fox jumped over a lazy dog.");
        revised.add("This sentence stays the same");
        revised.add("And this has been newly added.");
        revised.add("This will be the same.");
        
        // Compute diff. Get the Patch object. Patch is the container for computed deltas.
        difflib.Patch patch = DiffUtils.diff(original, revised);

        System.out.println(patch.getDeltas().size());
        
        for (Delta delta: patch.getDeltas()) {
                System.out.println(delta + "\t" + delta.getType() + delta.getOriginal().getLines());
        }
	}
}
