package plutocore

trait PlutoCoreConfigProvider {
  def get():Either[String,PlutoCoreConfig]
}

