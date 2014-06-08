package other;

import java.util.ArrayList;
import java.util.Arrays;
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
		original.add("The quick brown fox jumps over the lazy dog");
		original.add("This sentence stays the same");
		original.add("But this will be removed.");
		original.add("This will be the same.");

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

	@Test
	public void testDiffWords() {
		String s1 = "The quick brown fox jumps over the lazy dog. " +
				"This sentence stays the same. But this will be removed. " +
				"This will be the same.";
		String s2 = "And this has been added. That quick brown fox jumped over a lazy dog. " +
				"This sentence stays the same. " +
				"And this has been newly added. This will be the same.";

		String[] l1 = s1.split("\\s");
		String[] l2 = s2.split("\\s");
		difflib.Patch patch = DiffUtils.diff(Arrays.asList(l1), Arrays.asList(l2));
		System.out.println(patch.getDeltas().size());

		for (Delta delta: patch.getDeltas()) {
			System.out.println(delta.getRevised().getLines() + "\t" + delta.getType());
		}
	}
	
	@Test
	public void testWholeInsert() {
		String s1 = "";
		String s2 = "And this has been added. That quick brown fox jumped over a lazy dog. " +
				"This sentence stays the same. " +
				"And this has been newly added. This will be the same.";

		String[] l1 = s1.split("\\s");
		String[] l2 = s2.split("\\s");
		difflib.Patch patch = DiffUtils.diff(Arrays.asList(l1), Arrays.asList(l2));
		System.out.println(patch.getDeltas().size());

		for (Delta delta: patch.getDeltas()) {
			System.out.println(delta.getRevised().getLines() + "\t" + delta.getType());
		}
	}
}
