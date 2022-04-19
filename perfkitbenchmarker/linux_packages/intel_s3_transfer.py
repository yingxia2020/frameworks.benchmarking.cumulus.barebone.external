import logging
import os
import posixpath

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import aws_credentials
from absl import flags

FLAGS = flags.FLAGS


def GetFileFromS3(vm, s3_path, dest, try_aws_cli=True, bucket="cumulus"):
  """
  Utility function to transfer files stored at AWS S3 to SUT

  Args:
    vm: VirtualMachine. The VM to transfer the AWS S3 file to
    s3_path: s3 file path, such as emon/sep_private_linux_pkb.tar.bz2
    dest: destination file path, such as /opt/emon/sep_private_linux_pkb.tar.bz2
    try_aws_cli: if first try with AWS s3 copy or not, default value is True.
      Set it to False if the file to transfer has small file size
    bucket: S3 bucket name, default value is cumulus
  """

  s3_url_curl_command_template = 'curl https://{0}.s3.amazonaws.com/{1} -o {2} --write-out "%{{http_code}}" --silent'
  s3_url_curl_command_to_vm = s3_url_curl_command_template.format(
      bucket, s3_path, dest)
  # First try AWS S3 copy
  if try_aws_cli and vm.CLOUD != "Static":
    logging.info("Fetching file from remote repo using AWS S3 CLI")
    s3_cp_command = "aws s3 cp s3://{0}/{1} {2}".format(bucket, s3_path, dest)
    try:
      # only install awscli and aws credentials if they are never installed before
      if not aws_credentials.IsInstalled(vm):
        vm.Install('aws_credentials')
        vm.Install('awscliv2')
      _, _, retcode = vm.RemoteCommandWithReturnCode(s3_cp_command)
      return True
    except (data.ResourceNotFound, errors.VirtualMachine.RemoteCommandError,
            RuntimeError):
      logging.warning('Could not pull {} from bucket {} using S3 cp!'.format(
          s3_path, bucket))

  # Second try, if could download file to SUT directly with curl
  logging.info("Fetching file from remote repo using curl to remote SUT")
  curl_http_code, _, retcode = vm.RemoteCommandWithReturnCode(
      s3_url_curl_command_to_vm, ignore_failure=True)
  if curl_http_code == '200':
    return True
  else:
    # Last try, first download file to local then copy to SUT
    logging.warning('Could not pull {} from bucket {} using curl!'.format(
        s3_path, bucket))
    logging.info(
        "Fetching file from remote repo using curl to control machine then send to remote SUT"
    )
    filename = os.path.basename(dest)
    local_path = posixpath.join(FLAGS.temp_dir, filename)
    if not os.path.exists(local_path):
      curl_http_code, _, retcode = vm_util.IssueCommand(
          s3_url_curl_command_template.format(bucket, s3_path,
                                              local_path).split(),
          timeout=18000)
      # IssueCommand differs from vm.RemoteCommand and will add escaped quotes around curl's --write-out arg
      if curl_http_code != '\"200\"':
        logging.warning(
            'Could not pull {} from bucket {} to PKB host using curl!'.format(
                s3_path, bucket))
        raise RuntimeError(
            'All attempts to download database from s3 bucket {} failed!'.
            format(bucket))
    vm.PushFile(local_path, dest)
    vm_util.IssueCommand(['rm', local_path])
    return True


def GetFileFromS3WithUriString(vm, s3_uri_path, dest, try_aws_cli=True):
  """
  Wrapper of GetFileFromS3

  Args:
    vm: VirtualMachine. The VM to transfer the AWS S3 file to
    s3_uri_path: s3 uri string, such as s3://cumulus/emon/sep_private_linux_pkb.tar.bz2
    dest: destination file path, such as /opt/emon/sep_private_linux_pkb.tar.bz2
    try_aws_cli: if first try with AWS s3 copy or not, default value is True.
      Set it to False if the file to transfer has small file size
  """
  s3_bucket, s3_key = s3_uri_path.replace("s3://", "").split("/", 1)

  return GetFileFromS3(vm, s3_key, dest, try_aws_cli=try_aws_cli, bucket=s3_bucket)
