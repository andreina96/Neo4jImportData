/*
 * Creado por: Andreína Alvarado González
 * 11 de octubre del 2016
 * Propósito: carga los datos del archivo "ign.csv" para crear nodos y relaciones en la base de datos Neo4j
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
	 * enum para las etiquetas de los nodos que se usarán
	 * */
	public enum nodos implements Label{
		VIDEOJUEGO, PLATAFORMA, GENERO;
	}
	
	/*
	 * enum para los tipos de relaciones que se usarán
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
				new File("C:/Users/Andreína Alvarado/Desktop/db"));
		
		/*
		 * csvArchivo: contiene la ruta del archivo
		 * linea: guarda cada fila en el archivo
		 * cvsSeparador: caracter para partir cada una de las filas
		 */
		String csvArchivo = "C:/Users/Andreína Alvarado/eclipse/workspace/Neo4jImportDataPrueba/datos/ign.csv";
        String linea = "";
        String cvsSeparador = ",";
        
		/*
		 * Se separan en dos transacciones, porque no se pueden hacer operaciones en el schema y 
		 * operaciones sobre datos en la misma transacción
		 */
        try(Transaction tx = service.beginTx()){
        	/*
        	 * Se especifica que
        	 *  -los nodos con la etiqueta VIDEOJUEGO tendrán el título único
        	 *  -los nodos con la etiqueta PLATAFORMA tendrán el nombre único
        	 *  -los nodos con la etiqueta GENERO tendrán el nombre único
        	 */
        	service.schema().constraintFor(nodos.VIDEOJUEGO).assertPropertyIsUnique("Título").create();
            service.schema().constraintFor(nodos.PLATAFORMA).assertPropertyIsUnique("Nombre").create();
            service.schema().constraintFor(nodos.GENERO).assertPropertyIsUnique("Nombre").create();
            
            tx.success();
        }
        
		try(Transaction tx = service.beginTx()){
			/*
			 * Se lee línea por línea el archivo
			 */
			try (BufferedReader br = new BufferedReader(new FileReader(csvArchivo))) {
	            while ((linea = br.readLine()) != null) {
	                String[] columnas = linea.split(cvsSeparador);
	                
	                /*
	                 * Se verifica que el nodo VIDEOJUEGO que se desea agregar no exista y si no existe
	                 * se agrega
	                 */
	                Node videojuego = service.findNode(nodos.VIDEOJUEGO, "Título", columnas[2]);
	                if(videojuego == null){
	                	videojuego = service.createNode(nodos.VIDEOJUEGO);
		                videojuego.setProperty("Título", columnas[2]);
		                videojuego.setProperty("Año de lanzamiento", columnas[9]);
		                videojuego.setProperty("Puntuación", columnas[5]);
		                videojuego.setProperty("Frase de puntuación", columnas[1]);
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
