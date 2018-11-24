package it.uniroma3.newswire.utils.xpath;

import java.util.List;

import scala.Tuple2;

public class XPath {
	public String xpath;
	public List<Tuple2<String, Integer>> elements;
	
	public XPath(String xpath) {
		this.xpath = xpath;
		this.elements = XPathElementsGenerator.generate(xpath);
	}
	
	public boolean isSameCollection(XPath that) {
		if(this.elements.size()!=that.elements.size()) {
			return false;
		}
	
		for(int i = 0; i < this.elements.size(); ++i) {

			String thisSelector = this.elements.get(i)._1;
			String thatSelector = that.elements.get(i)._1;
			
			int thisPosition = this.elements.get(i)._2;
			int thatPosition = that.elements.get(i)._2;
			
			if(thisSelector.equals(thatSelector))
				if(thisPosition!=thatPosition) {
					double currentPos = (double) i + 1;
					double elements = (double) this.elements.size();

					if(currentPos/elements >= 0.75) {
						return true;
					}
						
					else {
						return false;
					
					}
				}
		}
		return false;
	}
	
	public String toString() {
		return this.xpath;
	}
	
	public boolean equals(Object that) {
		return this.xpath.equals(((XPath)that).xpath);
	}
	
	public int hashCode() {
		return this.xpath.hashCode();
	}
}
