/*
 * Creado por: Andre�na Alvarado Gonz�lez
 * 11 de octubre del 2016
 * Prop�sito: carga los datos del archivo "ign.csv" para crear nodos y relaciones en la base de datos Neo4j
 * */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Main {
	/*
	 * enum para las etiquetas de los nodos que se usar�n
	 * */
	public enum nodos implements Label{
		VIDEOJUEGO, PLATAFORMA, GENERO;
	}
	
	/*
	 * enum para los tipos de relaciones que se usar�n
	 */
	public enum relaciones implements RelationshipType{
		ESTA_DISPONIBLE_PARA, PERTENECE_AL_GENERO;
	}
	
	public static void main(String args[]){
		/*
		 * Se crea la instancia de la base de datos
		 */
		GraphDatabaseFactory factory = new GraphDatabaseFactory();
		GraphDatabaseService service = factory.newEmbeddedDatabase(
				new File("C:/Users/Andre�na Alvarado/Desktop/db"));
		
		/*
		 * csvArchivo: contiene la ruta del archivo
		 * linea: guarda cada fila en el archivo
		 * cvsSeparador: caracter para partir cada una de las filas
		 */
		String csvArchivo = "C:/Users/Andre�na Alvarado/eclipse/workspace/Neo4jImportDataPrueba/datos/ign.csv";
        String linea = "";
        String cvsSeparador = ",";
        
		/*
		 * Se separan en dos transacciones, porque no se pueden hacer operaciones en el schema y 
		 * operaciones sobre datos en la misma transacci�n
		 */
        try(Transaction tx = service.beginTx()){
        	/*
        	 * Se especifica que
        	 *  -los nodos con la etiqueta VIDEOJUEGO tendr�n el t�tulo �nico
        	 *  -los nodos con la etiqueta PLATAFORMA tendr�n el nombre �nico
        	 *  -los nodos con la etiqueta GENERO tendr�n el nombre �nico
        	 */
        	service.schema().constraintFor(nodos.VIDEOJUEGO).assertPropertyIsUnique("T�tulo").create();
            service.schema().constraintFor(nodos.PLATAFORMA).assertPropertyIsUnique("Nombre").create();
            service.schema().constraintFor(nodos.GENERO).assertPropertyIsUnique("Nombre").create();
            
            tx.success();
        }
        
		try(Transaction tx = service.beginTx()){
			/*
			 * Se lee l�nea por l�nea el archivo
			 */
			try (BufferedReader br = new BufferedReader(new FileReader(csvArchivo))) {
	            while ((linea = br.readLine()) != null) {
	                String[] columnas = linea.split(cvsSeparador);
	                
	                /*
	                 * Se verifica que el nodo VIDEOJUEGO que se desea agregar no exista y si no existe
	                 * se agrega
	                 */
	                Node videojuego = service.findNode(nodos.VIDEOJUEGO, "T�tulo", columnas[2]);
	                if(videojuego == null){
	                	videojuego = service.createNode(nodos.VIDEOJUEGO);
		                videojuego.setProperty("T�tulo", columnas[2]);
		                videojuego.setProperty("A�o de lanzamiento", columnas[9]);
		                videojuego.setProperty("Puntuaci�n", columnas[5]);
		                videojuego.setProperty("Frase de puntuaci�n", columnas[1]);
	                }
	                
	                /*
	                 * Se verifica que el nodo PLATAFORMA que se desea agregar no exista y si no existe
	                 * se agrega
	                 */
	                Node plataforma = service.findNode(nodos.PLATAFORMA, "Nombre", columnas[4]);
	                if(plataforma == null){
	                	plataforma = service.createNode(nodos.PLATAFORMA);
	                	plataforma.setProperty("Nombre", columnas[4]);
	                }
	                
	                /*
	                 * Se verifica que el nodo GENERO que se desea agregar no exista y si no existe
	                 * se agrega
	                 */
	                Node genero = service.findNode(nodos.GENERO, "Nombre", columnas[6]);
	                if(genero == null){
	                	genero = service.createNode(nodos.GENERO);
	                	genero.setProperty("Nombre", columnas[6]);
	                }
	                
	                /*
	                 * Se crean las relaciones con los nodos anteriores
	                 */
	                videojuego.createRelationshipTo(plataforma, relaciones.ESTA_DISPONIBLE_PARA);
	                videojuego.createRelationshipTo(genero, relaciones.PERTENECE_AL_GENERO);
	            }
	        }
			catch (IOException e) {
	            e.printStackTrace();
	        }
			tx.success();
		}
		catch(Exception ex){
			System.out.println(ex);
		}
	}
}
