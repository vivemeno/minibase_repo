package xmlparser;

import global.GlobalConst;

import global.IntervalType;
import global.NodeTable;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

class XMLTree {
    String tagName;
    List<XMLTree>  children = null;
}

//using event based parser to support larger files
public class XMLToIntervalTable implements GlobalConst {
    public final static Vector<NodeTable> xmlToTreeConverter() {
        XMLTree root = null;
        try {
            Stack<XMLTree> yetToCloseTags = new Stack<>();
            XMLInputFactory factory = XMLInputFactory.newInstance();
            factory.setProperty(XMLInputFactory.IS_COALESCING, true);
            XMLEventReader eventReader =
                    factory.createXMLEventReader(new FileReader("/home/akhil/MS/DBMS/sample.xml"));

            while(eventReader.hasNext()) {

                XMLEvent event = eventReader.nextEvent();

                switch(event.getEventType()) {

                    case XMLStreamConstants.START_ELEMENT:
                        StartElement startElement = event.asStartElement();
                        String tagName = startElement.getName().getLocalPart();
                        XMLTree node = new XMLTree();
                        node.tagName = trimCharTags(tagName);
                        Iterator<Attribute> attributes = startElement.getAttributes();
                        while(attributes.hasNext()) {
                            if(node.children == null) node.children = new ArrayList<>();
                            Attribute attr = attributes.next();
                            tagName = attr.getName().getLocalPart();
                            XMLTree attrNameNode = new XMLTree();
                            attrNameNode.tagName = trimCharTags(tagName);
                            XMLTree attrValNode = new XMLTree();
                            attrValNode.tagName = trimCharTags(attr.getValue());
                            attrNameNode.children = Arrays.asList(attrValNode);
                            node.children.add(attrNameNode);
                        }
                        if(yetToCloseTags.size() == 0) root = node;
                        else {
                            XMLTree prevNode = yetToCloseTags.peek();
                            if(prevNode.children == null) prevNode.children = new ArrayList<>();
                            prevNode.children.add(node);
                        }
                        yetToCloseTags.add(node);
                        break;

                    case XMLStreamConstants.CHARACTERS:
                        Characters characters = event.asCharacters();
                        XMLTree newNode = new XMLTree();
                        if(characters.getData().trim().length() ==0) break;
                        newNode.tagName = trimCharTags(characters.getData());
                        XMLTree parentNode = yetToCloseTags.peek();
                        if(parentNode.children == null) parentNode.children = new ArrayList<>();
                        parentNode .children.add(newNode);
                        yetToCloseTags.peek();
                        break;

                    case XMLStreamConstants.END_ELEMENT:
                        yetToCloseTags.pop();
                        break;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return createNdeTblFrmXMLTree(root);
    }

    private final static Vector<NodeTable> createNdeTblFrmXMLTree(XMLTree root) {
        Vector<NodeTable> finalResult = new Vector<>();
        preOrder(root, finalResult, 0, 1);
        return finalResult;
    }

    private final static int preOrder(XMLTree root, Vector<NodeTable> result, int intrvlCounter, int level) {
        if (root == null)
            return intrvlCounter;
        NodeTable nt = new NodeTable();
        nt.nodename = root.tagName;
        intrvlCounter++;
        nt.interval = new IntervalType();
        nt.interval.s = intrvlCounter;
        nt.interval.l = level;
        if(root.children!=null) {
            for (XMLTree node : root.children)
                intrvlCounter = preOrder(node, result, intrvlCounter, level+1);
        }
        intrvlCounter++;
        nt.interval.e = intrvlCounter;
        result.add(nt);
        return intrvlCounter;
    }

    public final static String trimCharTags(String s) {
        StringBuilder characterToTag = new StringBuilder(s);
        if(characterToTag.length() > XML_PLAIN_TXT_CHAR_LMT) {
            characterToTag.setLength(XML_PLAIN_TXT_CHAR_LMT);
        }
        return characterToTag.toString();
    }
}
