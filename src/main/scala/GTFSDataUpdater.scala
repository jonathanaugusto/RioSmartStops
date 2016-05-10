/**
  * @author jonny
  *
  *         UFRJ - Escola Politécnica
  *         "Big Data" course
  *         Professor: Villas Boas, Sergio B. [sbVB]
  *         Student: Jonathan Augusto <jonathanaugusto@poli.ufrj.br>
  *         Project: RioSmartStops
  *         Date: 23/06/15
  */

package RioSmartStops

import RioSmartStops.Functions._
import RioSmartStops.Global._


// GPS Data Updater Program
object GTFSDataUpdater {

  def main(args: Array[String]) {

    UpdateGTFSData()


    //FindNearBuses(-22.876705,-43.335793,200) // Viaduto de Madureira
    //FindNearBuses(-23.001494, -43.366088,200) // Alvorada
    //FindNearBuses(-22.901285, -43.179065,200) // Candelaria
    //FindNearBuses(-22.860928, -43.227278,200) // Bloco H

    stop()
  }

}

