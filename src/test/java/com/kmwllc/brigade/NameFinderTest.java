package com.kmwllc.brigade;

import com.kmwllc.brigade.config.StageConfig;
import com.kmwllc.brigade.document.Document;
import com.kmwllc.brigade.stage.NameFinder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by matt on 3/31/17.
 */
public class NameFinderTest {

    @Test
    public void testNameFinder() {
        String input = "Joe Smith, Steve Jones & Mark Jackson are all common names";

        Document testDoc = getDocument(input, false);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(3, output.size());
        assertEquals(true, output.contains("Joe Smith"));
        assertEquals(true, output.contains("Steve Jones"));
        assertEquals(true, output.contains("Mark Jackson"));
    }

    private Document getDocument(String input, boolean unique) {
        StageConfig stageConfig = new StageConfig("test", "test");
        Map<String, String> ioMap = new HashMap<>();
        ioMap.put("input", "output");
        stageConfig.setMapParam("ioMap", ioMap);
        stageConfig.setBoolParam("unique", unique);

        NameFinder nf = new NameFinder();
        nf.startStage(stageConfig);
        Document testDoc = new Document("abc");
        testDoc.setField("input", input);
        nf.processDocument(testDoc);
        return testDoc;
    }

    @Test
    public void testNameFinderNonUnique() {
        String input = "Joe Smith, Joe Smith & Mark Jackson are all common names";

        Document testDoc = getDocument(input, false);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(3, output.size());
        assertEquals(true, output.contains("Joe Smith"));
        assertEquals(true, output.contains("Mark Jackson"));
    }

    @Test
    public void testNameFinderUnique() {
        String input = "Joe Smith, Joe Smith & Mark Jackson are all common names";

        Document testDoc = getDocument(input, true);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(2, output.size());
        assertEquals(true, output.contains("Joe Smith"));
        assertEquals(true, output.contains("Mark Jackson"));
    }

    @Test
    public void testNameFinderMultiField() {
        String input = "Joe Smith, Joe Smith & Mark Jackson are all common names";
        String input2 = "So are Matt Jones and Dave Miller";

        StageConfig stageConfig = new StageConfig("test", "test");
        Map<String, String> ioMap = new HashMap<>();
        ioMap.put("input", "output");
        ioMap.put("input2", "output");
        stageConfig.setMapParam("ioMap", ioMap);
        stageConfig.setBoolParam("unique", true);

        NameFinder nf = new NameFinder();
        nf.startStage(stageConfig);
        Document testDoc = new Document("abc");
        testDoc.setField("input", input);
        testDoc.setField("input2", input2);
        nf.processDocument(testDoc);

        ArrayList<Object> output = testDoc.getField("output");
        assertEquals(4, output.size());
        assertEquals(true, output.contains("Joe Smith"));
        assertEquals(true, output.contains("Mark Jackson"));
        assertEquals(true, output.contains("Matt Jones"));
        assertEquals(true, output.contains("Dave Miller"));
    }

    //@Test
    public void test3rdi() {
        String input = "Russian swimmers Mikhail Dovgalyuk, Natalia Lovtcova, Anastasia Krapivina were also withdrawn. " +
                "Nikita Lobintsev, Vladimir Morozov and Daria Ustinova are ineligible to compete in Rio as their " +
                "names appeared in a recent World Anti-Doping Agency (WADA). Independent Person report. Donald Trump " +
                "told a rally in Columbus, Ohio, that he had heard \"more and more\" that the contest would be unfair." +
                " At another event he called Democratic rival Hillary Clinton \"the devil\".  Mr Trump has come under " +
                "fire from across the political divide for remarks he made about the parents of a US Muslim soldier " +
                "killed in action. The parachutists were laid out in front of the church to be identified by the man " +
                "who had betrayed them, fellow paratrooper Karel Curda. You can even see where they are - look - the " +
                "earth has been compressed a little,\" said Jiri Linek, from the Organisation of Former Political " +
                "Prisoners.  President Tony Tan might see his name written as \"Tony Tan Keng Yam\". A famous bearer of " +
                "this name is the sadly disappeared singer & actress Aaliyah Dana Houghton, known simply as Aaliyah " +
                "(1979-2001). The leading architects of this movement were Vedat Tek (1873�1942), Mimar Kemaleddin Bey " +
                "(1870�1927) and Arif Hikmet Koyunoglu (1888�1982). The 13th and current President is Pranab Mukherjee, " +
                "who was elected on 22 July 2012, and sworn in on 25 July 2012. Radhakrishnan was awarded several high " +
                "awards during his life, including a knighthood in 1931. \n" +
                "Upon gaining independence it became a one-party state under the presidency of Hastings Banda, who " +
                "remained president until 1994, when he lost an election. Peter Mutharika is the current president.  " +
                "Contemporary Irish visual artists of note include Sean Scully, Kevin Abosch, and Alice Maher. The Irish " +
                "philosopher and theologian Johannes Scotus Eriugena was considered one of the leading intellectuals of " +
                "his early Middle Ages. Sir Ernest Henry Shackleton, an Irish explorer, was one of the principal " +
                "figures of Antarctic exploration.  His daughter Liadh Ni Riada was elected as Sinn Fein European " +
                "Parliament member in 2014. The Anuradhapura Kingdom was established in 380 BCE during the reign of " +
                "Pandukabhaya of Anuradhapura.   The free education system established in 1945, is a result of the " +
                "initiative of C. W. W. Kannangara and A. Ratnayake. Thaksin Shinawatra (born 26 July 1949) is a Thai " +
                "businessman and politician. The reign of Vytautas the Great marked both the greatest territorial " +
                "expansion of the Grand Duchy and the defeat of the Teutonic Knights in the Battle of Grunwald in 1410. " +
                "Semyon Konstantinovich Timoshenko  (18 February 1895 � 31 March 1970) was a Soviet military commander " +
                "and Marshal of the Soviet Union. Adam Mickiewicz was a strong advocate of Poland's heritage during his " +
                "years in exile, 1798�1855. \n" +
                "This tradition was broken by Jan Kochanowski, who became one of the first Polish Renaissance authors to " +
                "write most of his works in Polish, along with Mikolaj Rej. Also, notable are the 19th and 20th-century " +
                "Polish authors such as Boleslaw Prus, Kornel Makuszynski, Stanislaw Lem, and Witold Gombrowicz among " +
                "others.  In the spirit of the notion of Adolf Ivar Arwidsson (1791�1858), \"we are no-longer Swedes, " +
                "we do not want to become Russians, let us therefore be Finns\", the Finnish national identity started " +
                "to establish. This was extensively exploited by president Urho Kekkonen against his opponents. In 2008, " +
                "president Martti Ahtisaari was awarded the Nobel Peace Prize. This allowed Oda Nobunaga to obtain " +
                "European technology and firearms, which he used to conquer many other daimyo.  Emperor Go-Daigo was " +
                "himself defeated by Ashikaga Takauji in 1336. Diplomatically, Ban Ki-moon has taken particularly strong " +
                "views on global warming, pressing the issue repeatedly with U.S. President George W. Bush, and on the " +
                "Darfur conflict, where he helped persuade Sudanese president Omar al-Bashir to allow peacekeeping " +
                "troops to enter Sudan.";

        Document testDoc = getDocument(input, true);

        ArrayList<Object> output = testDoc.getField("output");
        System.out.println(String.format("Found %d", output.size()));
        for (Object o : output) {
            System.out.println(o.toString());
        }
    }

    //@Test
    public void testGSW() {
        String input = "Warriors Roster: " +
                "Matt Barnes, " +
                "Ian Clark, " +
                "Stephen Curry, " +
                "Kevin Durant, " +
                "Draymond Green, " +
                "Andre Iguodala, " +
                "Damian Jones, " +
                "Shaun Livingston, " +
                "Kevon Looney, " +
                "James Michael McAdoo, " +
                "Patrick McCaw, " +
                "JaVale McGee, " +
                "Zaza Pachulia, " +
                "Klay Thompson, " +
                "David West";

        Document testDoc = getDocument(input, true);

        ArrayList<Object> output = testDoc.getField("output");
        System.out.println(String.format("Found %d of 15", output.size()));
        for (Object o : output) {
            System.out.println(o.toString());
        }
    }
}
