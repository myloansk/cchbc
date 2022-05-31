from abc import ABC, abstractmethod
import keras.preprocessing.image as ImageDataGenerator
import tensorflow as tf


class DeepNet(ABC):

    def __init__(self, Dframe, params:Dictionary ) -> None:
        self.Dframe = Dframe
        self.params = params
        self.deep_net_obj = None

    @abstractmethod
    def init_net(self) -> None:
        pass

   @abstractmethod
    def add_layer(self) -> None:
        pass

    def build_net(self) ->None:
        pass

    def pre_processing_set(self):
        pass


class ConvNet(DeepNet):
    def init_net(self):
        self.deep_net_obj = tf.keras.models.Sequential()

    def pre_processing_set(self):
        pass

    def add_layer(self):
        self.deep_net_obj.add(tf.keras.layers.Conv2D(filter=32,
                                                     kernel_size=3,
                                                     activation='relu',
                                                     input_shape = [64,64,3]))


class RNet(DeepNet):
    pass

class SomNet(DeepNet):
    pass

class BoltNet(DeepNet):
    pass
