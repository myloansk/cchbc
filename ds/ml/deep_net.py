from abc import ABC, abstractmethod
import keras.preprocessing.image as ImageDataGenerator
import tensorflow as tf


class DeepNet(ABC):

    def __init__(self, Dframe, params:Dictionary ) -> None:
        self.Dframe = Dframe
        self.params = params
        self.deep_net_obj = None

    def net_init(self) -> None:
        pass

   @abstractmethod
    def add_layer(self) -> None:
        pass

    def deep_net_blueprint(self) ->None:
        pass


class ConvNet(DeepNet):
    pass

class RNet(DeepNet):
    pass
