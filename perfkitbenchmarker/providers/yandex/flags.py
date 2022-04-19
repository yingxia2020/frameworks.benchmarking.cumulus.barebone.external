from absl import flags


def checkIfIn(collection):
    def fun(value):
      if not value:
        return False
      for item in value:
        if item not in collection:
          return False
      return True

    return fun

FLAGS = flags.FLAGS

flags.DEFINE_string('yandex_folder_name', 'pkb', 'Yandex folder name to put in vm')
flags.DEFINE_string('yandex_cloud_id', None, 'Id of cloud to operate on')
flags.DEFINE_integer('yandex_boot_disk_size', 50, 'Size in GBs of boot disk')
flags.DEFINE_integer('yandex_cores', 8, 'Count of cores assigned to created VM')
flags.DEFINE_integer('yandex_memory', 16, 'Size of memory in GBs')
flags.DEFINE_string('yandex_image', "", 'Image family for created VM')
flags.DEFINE_string('yandex_platform', 'standard-v2', 'Type of platform. Choices are standard-v1 - Broadwell, '
                    'standard-v2 - CLX, standard-v3 - ICX')

flags.DEFINE_string("yandex_zone", "ru-central1-a", "Zone to deploy in")
