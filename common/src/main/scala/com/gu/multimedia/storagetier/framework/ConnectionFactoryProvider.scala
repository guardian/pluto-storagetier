package com.gu.multimedia.storagetier.framework

import com.rabbitmq.client.ConnectionFactory

trait ConnectionFactoryProvider {
  def get():ConnectionFactory
}

object ConnectionFactoryProviderReal extends ConnectionFactoryProvider {
  override def get(): ConnectionFactory = new ConnectionFactory()
}