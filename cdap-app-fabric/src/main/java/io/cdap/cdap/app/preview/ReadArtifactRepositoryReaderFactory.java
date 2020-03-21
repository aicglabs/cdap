package io.cdap.cdap.app.preview;

import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.LocalArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;

public class ReadArtifactRepositoryReaderFactory implements ArtifactRepositoryReaderFactory{

  private final CConfiguration cConf;
  private final Injector injector;


  @Inject
  public ReadArtifactRepositoryReaderFactory(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }


  @Override
  public ArtifactRepositoryReader create() {
    String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
    if (storageImpl == null) {
      throw new IllegalStateException("No storage implementation is specified in the configuration file");
    }

    storageImpl = storageImpl.toLowerCase();
    if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_NOSQL)) {
      return injector.getInstance(RemoteArtifactRepositoryReader.class);
    }
    if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
      return injector.getInstance(LocalArtifactRepositoryReader.class);
    }
    throw new UnsupportedOperationException(
      String.format("%s is not a supported storage implementation, the supported implementations are %s and %s",
                    storageImpl, Constants.Dataset.DATA_STORAGE_NOSQL, Constants.Dataset.DATA_STORAGE_SQL));
  }
}

